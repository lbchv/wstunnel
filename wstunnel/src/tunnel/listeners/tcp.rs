use crate::protocols;
use crate::tunnel::{LocalProtocol, RemoteAddr};
use anyhow::{Context, anyhow};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Poll, ready};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use url::Host;

pub struct TcpTunnelListener {
    listener: TcpListenerStream,
    dest: (Host, u16),
    proxy_protocol: bool,
}

impl TcpTunnelListener {
    pub async fn new(bind_addr: SocketAddr, dest: (Host, u16), proxy_protocol: bool) -> anyhow::Result<Self> {
        let listener = protocols::tcp::run_server(bind_addr, false)
            .await
            .with_context(|| anyhow!("Cannot start TCP server on {bind_addr}"))?;

        Ok(Self {
            listener,
            dest,
            proxy_protocol,
        })
    }
}

impl Stream for TcpTunnelListener {
    type Item = anyhow::Result<((OwnedReadHalf, OwnedWriteHalf), RemoteAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let ret = ready!(Pin::new(&mut this.listener).poll_next(cx));
        let ret = match ret {
            Some(Ok(strean)) => {
                let (host, port) = this.dest.clone();
                let peer_addr = strean.peer_addr().ok();
                let (src_host, src_port) = peer_addr
                    .map(|addr| {
                        let host = match addr.ip() {
                            std::net::IpAddr::V4(ip) => url::Host::Ipv4(ip),
                            std::net::IpAddr::V6(ip) => url::Host::Ipv6(ip),
                        };
                        (Some(host), Some(addr.port()))
                    })
                    .unwrap_or((None, None));

                Some(anyhow::Ok((
                    strean.into_split(),
                    RemoteAddr {
                        protocol: LocalProtocol::Tcp {
                            proxy_protocol: this.proxy_protocol,
                        },
                        host,
                        port,
                        src_host,
                        src_port,
                        dest_host: None,
                        dest_port: None,
                    },
                )))
            }
            Some(Err(err)) => Some(Err(anyhow::Error::new(err))),
            None => None,
        };
        Poll::Ready(ret)
    }
}
