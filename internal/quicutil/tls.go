// Package quicutil holds small TLS helpers shared by the lighthouse and the
// quicchat client.
//
// QUIC *always* runs inside TLS 1.3 — there is no such thing as an unencrypted
// QUIC connection. That means every QUIC endpoint (server OR peer) needs a
// certificate. For a learning POC we don't care about a real chain of trust, so
// we generate a throwaway self-signed certificate on startup and tell the other
// side to skip verification. In production you would use a real certificate
// (e.g. from Let's Encrypt, which is exactly what Traefik does for the
// lighthouse).
package quicutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"
)

// GenerateTLSConfig builds a *tls.Config carrying a brand-new self-signed
// certificate. The alpn arguments are the ALPN ("Application-Layer Protocol
// Negotiation") identifiers advertised during the TLS handshake. ALPN is how a
// single QUIC socket can tell "this incoming connection wants to speak HTTP/3
// (h3)" apart from "this one wants to speak our chat protocol".
//
// This config is meant for the *server/listener* side (it carries a cert).
func GenerateTLSConfig(alpn ...string) *tls.Config {
	// 1. Generate a private key. ECDSA P-256 is small and fast.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	// 2. Describe the certificate. None of these fields matter for a POC that
	//    skips verification, but x509 requires a serial number and validity
	//    window.
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "quickmesh-poc"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
	}

	// 3. Self-sign: the template is both the certificate and its own issuer.
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	// 4. Encode key + cert to PEM and load them back as a tls.Certificate.
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   alpn,
		MinVersion:   tls.VersionTLS13,
	}
}

// ClientTLSConfig builds the *client/dialer* side config. It carries no
// certificate of its own; it only declares which ALPN protocol it wants to
// speak.
//
// insecure=true skips verification of the server's certificate. This is
// REQUIRED for peer-to-peer connections (peers use throwaway self-signed certs)
// and is convenient for talking to a self-signed lighthouse on a LAN. Set it to
// false to properly verify a real certificate (e.g. Let's Encrypt), in which
// case serverName must match the certificate's domain.
func ClientTLSConfig(serverName string, insecure bool, alpn ...string) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		NextProtos:         alpn,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: insecure,
	}
}
