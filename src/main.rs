use std::{rc::Rc, sync::Arc};

use anyhow::{ensure, Context, Error, Result};
use dencode::{BufMut, BytesMut, Encoder, FramedWrite};
use futures::{stream::FuturesUnordered, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use sbp::messages::{SBPMessage, SBP};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
    time,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

type Reader = Box<dyn AsyncRead + Unpin + Send>;
type Writer = Box<dyn AsyncWrite + Unpin + Send>;
type Msg = Rc<SBP>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (src, dests) = get_inputs().await?;
    run(src, dests).await
}

async fn get_inputs() -> Result<(Reader, Vec<Writer>)> {
    let mut args = std::env::args().skip(1);

    let input_file = args.next().context("missing input file")?;
    let src = tokio::fs::File::open(input_file).map_ok(Box::new).await? as Reader;

    let mut dests = vec![];
    for p in args {
        let dest = tokio::fs::File::create(p).await?;
        dests.push(Box::new(dest) as Writer);
    }

    ensure!(!dests.is_empty(), "need at least one dest");

    Ok((src, dests))
}

async fn run(src: Reader, dests: Vec<Writer>) -> Result<()> {
    let stream = stream_messages(src);
    futures::pin_mut!(stream);

    let sinks = create_sinks(dests);

    let feed_sinks = Arc::new(Mutex::new(sinks));
    let flush_sinks = Arc::clone(&feed_sinks);

    let flusher = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut sinks = flush_sinks.lock().await;
            flush_all(&mut sinks).await;
        }
    });

    while let Some(msg) = stream.next().await {
        let mut sinks = feed_sinks.lock().await;
        feed_all(&mut sinks, msg).await;
    }

    drop(flusher);

    let mut sinks = feed_sinks.lock().await;
    flush_all(&mut sinks).await;

    Ok(())
}

fn stream_messages(src: Reader) -> impl Stream<Item = Msg> {
    let src = TokioAsyncReadCompatExt::compat(src);
    sbp::stream_messages(src).filter_map(|m| async {
        match m {
            Ok(msg) => Some(Rc::new(msg)),
            Err(e) => {
                eprintln!("error reading: {}", e);
                None
            }
        }
    })
}

fn create_sinks(dests: Vec<Writer>) -> Vec<impl Sink<Msg, Error = Error> + Unpin> {
    dests
        .into_iter()
        .map(|dest| {
            let encoder = RcSbpEncoder::new();
            let dest = TokioAsyncWriteCompatExt::compat_write(dest);
            FramedWrite::new(dest, encoder)
        })
        .collect()
}

async fn feed_all(sinks: &mut Vec<impl Sink<Msg, Error = Error> + Unpin>, msg: Msg) {
    let mut work: FuturesUnordered<_> = sinks.iter_mut().map(|s| s.feed(msg.clone())).collect();
    while let Some(res) = work.next().await {
        if let Err(e) = res {
            eprintln!("error feeding: {}", e);
        }
    }
}

async fn flush_all(sinks: &mut Vec<impl Sink<Msg, Error = Error> + Unpin>) {
    let mut work: FuturesUnordered<_> = sinks.iter_mut().map(|s| s.flush()).collect();
    while let Some(res) = work.next().await {
        if let Err(e) = res {
            eprintln!("error flushing {}", e);
        }
    }
}

// TODO: sbp::codec::sbp::SbpEncoder should work with &SBP, Rc<SBP> etc...
struct RcSbpEncoder {
    frame: Vec<u8>,
}

impl RcSbpEncoder {
    fn new() -> Self {
        RcSbpEncoder { frame: Vec::new() }
    }
}

impl Encoder for RcSbpEncoder {
    type Item = Msg;
    type Error = Error;

    fn encode(&mut self, msg: Msg, dst: &mut BytesMut) -> Result<()> {
        self.frame.clear();
        match msg.write_frame(&mut self.frame) {
            Ok(_) => dst.put_slice(self.frame.as_slice()),
            Err(err) => eprintln!("Error converting sbp message to frame: {}", err),
        }
        Ok(())
    }
}
