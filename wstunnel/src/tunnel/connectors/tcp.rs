use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use url::{Host, Url};

use crate::protocols;
use crate::protocols::dns::DnsResolver;
use crate::somark::SoMark;
use crate::tunnel::connectors::TunnelConnector;
use crate::tunnel::{LocalProtocol, RemoteAddr};

pub struct TcpTunnelConnector<'a> {
    host: &'a Host,
    port: u16,
    so_mark: SoMark,
    connect_timeout: Duration,
    dns_resolver: &'a DnsResolver,
}

impl<'a> TcpTunnelConnector<'a> {
    pub fn new(
        host: &'a Host,
        port: u16,
        so_mark: SoMark,
        connect_timeout: Duration,
        dns_resolver: &'a DnsResolver,
    ) -> TcpTunnelConnector<'a> {
        TcpTunnelConnector {
            host,
            port,
            so_mark,
            connect_timeout,
            dns_resolver,
        }
    }
}

impl TunnelConnector for TcpTunnelConnector<'_> {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn connect(&self, remote: &Option<RemoteAddr>) -> anyhow::Result<(Self::Reader, Self::Writer)> {
        let (host, port) = match remote {
            Some(remote) => {
                // For reverse tunnels, use dest_host/dest_port if available
                if let (Some(dest_host), Some(dest_port)) = (&remote.dest_host, remote.dest_port) {
                    (dest_host, dest_port)
                } else {
                    (&remote.host, remote.port)
                }
            }
            None => (self.host, self.port),
        };

        let mut stream =
            protocols::tcp::connect(host, port, self.so_mark, self.connect_timeout, self.dns_resolver).await?;

        // Send proxy protocol header if needed for reverse TCP tunnels
        if let Some(remote) = remote {
            let should_send_proxy_protocol = matches!(
                remote.protocol,
                LocalProtocol::ReverseTcp { proxy_protocol: true } | LocalProtocol::Tcp { proxy_protocol: true }
            );

            if should_send_proxy_protocol && let (Some(src_host), Some(src_port)) = (&remote.src_host, remote.src_port)
            {
                let src_addr = match src_host {
                    Host::Ipv4(ip) => SocketAddr::new(IpAddr::V4(*ip), src_port),
                    Host::Ipv6(ip) => SocketAddr::new(IpAddr::V6(*ip), src_port),
                    _ => SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), src_port),
                };

                let dst_addr = stream.local_addr()?;

                let header = ppp::v2::Builder::with_addresses(
                    ppp::v2::Version::Two | ppp::v2::Command::Proxy,
                    ppp::v2::Protocol::Stream,
                    (src_addr, dst_addr),
                )
                .build()?;

                stream.write_all(&header).await?;
            }
        }

        Ok(stream.into_split())
    }

    async fn connect_with_http_proxy(
        &self,
        proxy: &Url,
        remote: &Option<RemoteAddr>,
    ) -> anyhow::Result<(Self::Reader, Self::Writer)> {
        let (host, port) = match remote {
            Some(remote) => (&remote.host, remote.port),
            None => (self.host, self.port),
        };

        let stream = protocols::tcp::connect_with_http_proxy(
            proxy,
            host,
            port,
            self.so_mark,
            self.connect_timeout,
            self.dns_resolver,
        )
        .await?;
        Ok(stream.into_split())
    }
}
