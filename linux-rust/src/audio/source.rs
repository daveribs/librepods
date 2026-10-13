//! Publishes decoded AirPods microphone audio as a PipeWire source, so any
//! application can record from it while playback stays on A2DP.

use log::{error, info};
use pipewire as pw;
use pw::properties::properties;
use pw::spa;
use spa::pod::Pod;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const SAMPLE_SIZE: usize = std::mem::size_of::<i16>();

/// Upper bound on buffered audio, in milliseconds. Enough to absorb Bluetooth
/// jitter, small enough to be imperceptible. Anything older is dropped rather
/// than played late: a microphone that lags behind is worse than one that skips.
const MAX_QUEUED_MS: usize = 60;

/// PipeWire scheduling quantum requested for the node, in milliseconds.
const NODE_QUANTUM_MS: u32 = 20;

enum Command {
    Quit,
}

/// State shared between the decoder thread and the PipeWire thread.
struct Shared {
    queue: Mutex<VecDeque<i16>>,
    /// True only while an application is actually recording from the node.
    /// While false, audio is discarded instead of queued, so the first read
    /// never starts with a backlog of stale samples.
    streaming: AtomicBool,
}

pub struct MicSource {
    shared: Arc<Shared>,
    max_queued: usize,
    sender: pw::channel::Sender<Command>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MicSource {
    /// Starts the PipeWire node on its own thread.
    pub fn start(rate: u32, channels: u32) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            queue: Mutex::new(VecDeque::new()),
            streaming: AtomicBool::new(false),
        });
        let max_queued = rate as usize * channels as usize * MAX_QUEUED_MS / 1000;
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
            max_queued,
            sender,
            thread: Some(thread),
        })
    }

    /// Queues decoded samples for delivery to whoever is recording.
    pub fn push(&self, pcm: &[i16]) {
        if !self.shared.streaming.load(Ordering::Acquire) {
            return;
        }
        let Ok(mut queue) = self.shared.queue.lock() else {
            return;
        };
        queue.extend(pcm.iter().copied());
        if queue.len() > self.max_queued {
            // The reader fell behind. Dropping the oldest audio keeps latency
            // bounded; logging every overflow would flood the log at 130
            // frames a second.
            let excess = queue.len() - self.max_queued;
            queue.drain(..excess);
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

    let latency = format!("{}/{}", rate * NODE_QUANTUM_MS / 1000, rate);
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
            // Small quantum so PipeWire does not add its own buffering.
            *pw::keys::NODE_LATENCY => latency.as_str(),
        },
    )?;

    let stride = SAMPLE_SIZE * channels as usize;
    let _listener = stream
        .add_local_listener_with_user_data(shared)
        .state_changed(|_, shared, _old, new| {
            let streaming = matches!(new, pw::stream::StreamState::Streaming);
            if streaming {
                // Start from empty so the first read is live audio.
                if let Ok(mut queue) = shared.queue.lock() {
                    queue.clear();
                }
            }
            shared.streaming.store(streaming, Ordering::Release);
            if !streaming {
                if let Ok(mut queue) = shared.queue.lock() {
                    queue.clear();
                }
            }
        })
        .process(move |stream, shared| {
            let Some(mut buffer) = stream.dequeue_buffer() else {
                return;
            };
            let data = &mut buffer.datas_mut()[0];
            let Some(slice) = data.data() else {
                return;
            };

            let wanted = slice.len() / SAMPLE_SIZE;
            let mut written = 0;
            if let Ok(mut queue) = shared.queue.lock() {
                while written < wanted {
                    let Some(sample) = queue.pop_front() else {
                        break;
                    };
                    let start = written * SAMPLE_SIZE;
                    slice[start..start + SAMPLE_SIZE].copy_from_slice(&sample.to_le_bytes());
                    written += 1;
                }
            }
            // Silence rather than stale audio when the buds fall behind.
            for i in written..wanted {
                let start = i * SAMPLE_SIZE;
                slice[start..start + SAMPLE_SIZE].copy_from_slice(&0i16.to_le_bytes());
            }

            let chunk = data.chunk_mut();
            *chunk.offset_mut() = 0;
            *chunk.stride_mut() = stride as _;
            *chunk.size_mut() = (wanted * SAMPLE_SIZE) as _;
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
