//! Publishes decoded AirPods microphone audio as a PipeWire source, so any
//! application can record from it while playback stays on A2DP.
//!
//! Tuning knobs live in `~/.config/librepods/mic.conf` (or
//! `$XDG_CONFIG_HOME/librepods/mic.conf`) and are re-read about once a second,
//! so they can be changed while the app is running:
//!
//!   max_queue_ms = 200   # hard cap on buffered audio; older audio is dropped
//!   prebuffer_ms = 60    # audio collected before playback (re)starts
//!   gain         = 3.0   # linear volume multiplier, clipped at full scale

use log::{error, info, warn};
use pipewire as pw;
use pw::properties::properties;
use pw::spa;
use spa::pod::Pod;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

const SAMPLE_SIZE: usize = std::mem::size_of::<i16>();

const DEFAULT_MAX_QUEUE_MS: u32 = 200;
const DEFAULT_PREBUFFER_MS: u32 = 60;
const DEFAULT_GAIN_MILLI: u32 = 3000;

/// PipeWire scheduling quantum, in ms. Override with LIBREPODS_MIC_QUANTUM_MS
/// (read once when the node is created).
const DEFAULT_QUANTUM_MS: u32 = 20;

const DEFAULT_CONFIG: &str = "# LibrePods microphone tuning. Re-read about once a second.\n\
# Keep max_queue_ms at least twice prebuffer_ms.\n\
#\n\
# Hard cap on buffered audio. Raise it if the log shows many overruns.\n\
max_queue_ms = 200\n\
# Audio collected before playback starts or resumes after a gap.\n\
# Higher = fewer dropouts, more delay.\n\
prebuffer_ms = 60\n\
# Linear volume multiplier (1.0 = unchanged).\n\
gain = 3.0\n";

enum Command {
    Quit,
}

struct Tuning {
    max_queue_ms: AtomicU32,
    prebuffer_ms: AtomicU32,
    gain_milli: AtomicU32,
}

/// State shared between the decoder thread and the PipeWire thread.
struct Shared {
    queue: Mutex<VecDeque<i16>>,
    /// True only while an application is actually recording from the node.
    streaming: AtomicBool,
    /// False while the jitter buffer is refilling after start or an underrun.
    playing: AtomicBool,
    tuning: Tuning,
    /// Samples per second across all channels.
    samples_per_sec: usize,
    underruns: AtomicU64,
    overruns: AtomicU64,
}

struct Housekeeping {
    path: Option<PathBuf>,
    mtime: Option<SystemTime>,
    last_check: Instant,
    last_stats: Instant,
}

pub struct MicSource {
    shared: Arc<Shared>,
    housekeeping: Mutex<Housekeeping>,
    sender: pw::channel::Sender<Command>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("librepods").join("mic.conf"))
}

fn apply_config(tuning: &Tuning, text: &str) {
    let mut max_ms = DEFAULT_MAX_QUEUE_MS;
    let mut pre_ms = DEFAULT_PREBUFFER_MS;
    let mut gain = DEFAULT_GAIN_MILLI as f64 / 1000.0;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let (key, value) = (key.trim(), value.trim());
        match key {
            "max_queue_ms" => match value.parse::<u32>() {
                Ok(v) => max_ms = v.clamp(20, 2000),
                Err(_) => warn!("mic.conf: bad max_queue_ms {value:?}"),
            },
            "prebuffer_ms" => match value.parse::<u32>() {
                Ok(v) => pre_ms = v.min(1000),
                Err(_) => warn!("mic.conf: bad prebuffer_ms {value:?}"),
            },
            "gain" => match value.parse::<f64>() {
                Ok(v) => gain = v.clamp(0.1, 16.0),
                Err(_) => warn!("mic.conf: bad gain {value:?}"),
            },
            other => warn!("mic.conf: unknown key {other:?}"),
        }
    }
    pre_ms = pre_ms.min(max_ms);
    tuning.max_queue_ms.store(max_ms, Ordering::Relaxed);
    tuning.prebuffer_ms.store(pre_ms, Ordering::Relaxed);
    tuning
        .gain_milli
        .store((gain * 1000.0).round() as u32, Ordering::Relaxed);
    info!("Mic tuning: max_queue_ms={max_ms} prebuffer_ms={pre_ms} gain={gain:.2}");
}

impl MicSource {
    /// Starts the PipeWire node on its own thread.
    pub fn start(rate: u32, channels: u32) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            queue: Mutex::new(VecDeque::new()),
            streaming: AtomicBool::new(false),
            playing: AtomicBool::new(false),
            tuning: Tuning {
                max_queue_ms: AtomicU32::new(DEFAULT_MAX_QUEUE_MS),
                prebuffer_ms: AtomicU32::new(DEFAULT_PREBUFFER_MS),
                gain_milli: AtomicU32::new(DEFAULT_GAIN_MILLI),
            },
            samples_per_sec: rate as usize * channels as usize,
            underruns: AtomicU64::new(0),
            overruns: AtomicU64::new(0),
        });

        let path = config_path();
        let mut mtime = None;
        if let Some(p) = &path {
            if !p.exists() {
                if let Some(dir) = p.parent() {
                    let _ = std::fs::create_dir_all(dir);
                }
                let _ = std::fs::write(p, DEFAULT_CONFIG);
            }
            match std::fs::read_to_string(p) {
                Ok(text) => {
                    apply_config(&shared.tuning, &text);
                    mtime = std::fs::metadata(p).and_then(|m| m.modified()).ok();
                }
                Err(e) => warn!("Could not read {}: {e}", p.display()),
            }
            info!("Edit {} to tune the microphone live", p.display());
        }

        let (sender, receiver) = pw::channel::channel();
        let state = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("librepods-mic".into())
            .spawn(move || {
                if let Err(e) = run(state, receiver, rate, channels) {
                    error!("Microphone source stopped: {e}");
                }
            })?;

        Ok(Self {
            shared,
            housekeeping: Mutex::new(Housekeeping {
                path,
                mtime,
                last_check: Instant::now(),
                last_stats: Instant::now(),
            }),
            sender,
            thread: Some(thread),
        })
    }

    /// Reloads the config file and logs buffer statistics. Called per push;
    /// cheap because it only acts about once a second.
    fn housekeeping(&self) {
        let Ok(mut hk) = self.housekeeping.try_lock() else {
            return;
        };
        if hk.last_check.elapsed() >= Duration::from_secs(1) {
            hk.last_check = Instant::now();
            if let Some(path) = hk.path.clone() {
                let modified = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
                if modified.is_some() && modified != hk.mtime {
                    hk.mtime = modified;
                    match std::fs::read_to_string(&path) {
                        Ok(text) => apply_config(&self.shared.tuning, &text),
                        Err(e) => warn!("Could not read {}: {e}", path.display()),
                    }
                }
            }
        }
        if hk.last_stats.elapsed() >= Duration::from_secs(2) {
            hk.last_stats = Instant::now();
            if self.shared.streaming.load(Ordering::Acquire) {
                let queued = self.shared.queue.lock().map(|q| q.len()).unwrap_or(0);
                let ms = queued * 1000 / self.shared.samples_per_sec.max(1);
                let under = self.shared.underruns.swap(0, Ordering::Relaxed);
                let over = self.shared.overruns.swap(0, Ordering::Relaxed);
                info!("Mic buffer: {ms} ms queued, {under} underruns, {over} overruns (last 2 s)");
            }
        }
    }

    /// Queues decoded samples for delivery to whoever is recording.
    pub fn push(&self, pcm: &[i16]) {
        self.housekeeping();
        if !self.shared.streaming.load(Ordering::Acquire) {
            return;
        }
        let max_samples = self.shared.samples_per_sec
            * self.shared.tuning.max_queue_ms.load(Ordering::Relaxed) as usize
            / 1000;
        let Ok(mut queue) = self.shared.queue.lock() else {
            return;
        };
        queue.extend(pcm.iter().copied());
        if queue.len() > max_samples {
            // The reader fell behind. Dropping the oldest audio keeps latency
            // bounded.
            let excess = queue.len() - max_samples;
            queue.drain(..excess);
            self.shared.overruns.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for MicSource {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        info!("Microphone source removed");
    }
}

fn run(
    shared: Arc<Shared>,
    receiver: pw::channel::Receiver<Command>,
    rate: u32,
    channels: u32,
) -> Result<(), pw::Error> {
    pw::init();

    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;

    let quantum_ms = std::env::var("LIBREPODS_MIC_QUANTUM_MS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| (1..=200).contains(v))
        .unwrap_or(DEFAULT_QUANTUM_MS);
    let latency = format!("{}/{}", rate * quantum_ms / 1000, rate);
    info!("Mic node latency request: {latency}");

    let stream = pw::stream::StreamBox::new(
        &core,
        "librepods-mic",
        properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            // Audio/Source is what makes this node show up as a microphone.
            *pw::keys::MEDIA_CLASS => "Audio/Source",
            *pw::keys::NODE_NAME => "librepods_mic",
            *pw::keys::NODE_DESCRIPTION => "AirPods Microphone (LibrePods)",
            *pw::keys::NODE_LATENCY => latency.as_str(),
        },
    )?;

    let stride = SAMPLE_SIZE * channels as usize;
    let channel_count = channels as usize;
    let _listener = stream
        .add_local_listener_with_user_data(shared)
        .state_changed(|_, shared, _old, new| {
            let streaming = matches!(new, pw::stream::StreamState::Streaming);
            shared.playing.store(false, Ordering::Release);
            if let Ok(mut queue) = shared.queue.lock() {
                queue.clear();
            }
            shared.streaming.store(streaming, Ordering::Release);
        })
        .process(move |stream, shared| {
            // The safe Buffer wrapper does not expose how many frames the graph
            // asked for this cycle, so use the raw pw_buffer. Every path below
            // hands the buffer back with queue_raw_buffer.
            let raw = unsafe { stream.dequeue_raw_buffer() };
            if raw.is_null() {
                return;
            }
            let (requested, spa_data) = unsafe {
                let spa_buf = (*raw).buffer;
                ((*raw).requested as usize, (*spa_buf).datas)
            };
            let ptr = unsafe { (*spa_data).data } as *mut u8;
            if ptr.is_null() {
                unsafe { stream.queue_raw_buffer(raw) };
                return;
            }
            let max_bytes = unsafe { (*spa_data).maxsize } as usize;
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, max_bytes) };

            // Fill only what the graph asked for this cycle, not the whole
            // (possibly much larger) buffer, or every cycle underruns.
            let max_frames = slice.len() / stride;
            let frames = if requested > 0 {
                requested.min(max_frames)
            } else {
                max_frames
            };
            let wanted = frames * channel_count;

            let gain = shared.tuning.gain_milli.load(Ordering::Relaxed) as i64;
            let prebuffer = shared.samples_per_sec
                * shared.tuning.prebuffer_ms.load(Ordering::Relaxed) as usize
                / 1000;

            let mut written = 0;
            if let Ok(mut queue) = shared.queue.lock() {
                if !shared.playing.load(Ordering::Relaxed) && queue.len() >= prebuffer.max(wanted)
                {
                    shared.playing.store(true, Ordering::Relaxed);
                }
                if shared.playing.load(Ordering::Relaxed) {
                    while written < wanted {
                        let Some(sample) = queue.pop_front() else {
                            break;
                        };
                        let amplified = (sample as i64 * gain / 1000)
                            .clamp(i16::MIN as i64, i16::MAX as i64)
                            as i16;
                        let start = written * SAMPLE_SIZE;
                        slice[start..start + SAMPLE_SIZE]
                            .copy_from_slice(&amplified.to_le_bytes());
                        written += 1;
                    }
                    if written < wanted {
                        // Underrun: refill the jitter buffer before resuming.
                        shared.playing.store(false, Ordering::Relaxed);
                        shared.underruns.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            for i in written..wanted {
                let start = i * SAMPLE_SIZE;
                slice[start..start + SAMPLE_SIZE].copy_from_slice(&0i16.to_le_bytes());
            }

            unsafe {
                let chunk = (*spa_data).chunk;
                (*chunk).offset = 0;
                (*chunk).stride = stride as i32;
                (*chunk).size = (wanted * SAMPLE_SIZE) as u32;
                stream.queue_raw_buffer(raw);
            }
        })
        .register()?;

    let mut audio_info = spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(spa::param::audio::AudioFormat::S16LE);
    audio_info.set_rate(rate);
    audio_info.set_channels(channels);

    let values: Vec<u8> = pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(pw::spa::pod::Object {
            type_: pw::spa::sys::SPA_TYPE_OBJECT_Format,
            id: pw::spa::sys::SPA_PARAM_EnumFormat,
            properties: audio_info.into(),
        }),
    )
    .map_err(|_| pw::Error::CreationFailed)?
    .0
    .into_inner();
    let mut params = [Pod::from_bytes(&values).ok_or(pw::Error::CreationFailed)?];

    stream.connect(
        spa::utils::Direction::Output,
        None,
        pw::stream::StreamFlags::AUTOCONNECT
            | pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS,
        &mut params,
    )?;

    let _receiver = receiver.attach(mainloop.loop_(), {
        let mainloop = mainloop.clone();
        move |_| mainloop.quit()
    });

    info!("Microphone source published as \"AirPods Microphone (LibrePods)\" at {rate} Hz");
    mainloop.run();
    Ok(())
}
