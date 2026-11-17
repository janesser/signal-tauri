# Maintainer: jan <jan@...>
#
# A native Signal Desktop client built with egui + presage. Built on cyberkleiber (the
# fleet build host) and published to the lan-repo on the Pi: clients install via
# pacman without a rustc of their own -- cargo is too heavy for the target box.

pkgname=signal-tauri-bin
arch=(x86_64)
url='https://github.com/janesser/signal-tauri'
license=('MIT')
makedepends=(rust cargo git)
source=("signal-tauri::git+https://github.com/janesser/signal-tauri.git")

pkgver() {
  # mirror Cargo.toml's version and append the git commit so every upstream change
  # yields a new version (the rebuild trigger only rebuilds on bumps)
  local ver="$(grep -m1 '^version=' "$srcdir/signal-tauri/Cargo.toml" | cut -d= -f2)"
  printf '%s.r%s.g%s' "$ver" "$(git rev-list --count HEAD)" "$(git rev-parse --short HEAD)"
}

package() {
  # the container (archlinux:base-devel, --network host) resolves crates.io and the
  # presage git deps from github.com; cargo + rustc do their heavy lifting there.
  cargo build --release
  install -Dm755 target/release/signal-tauri "$pkgdir/usr/bin/signal-tauri"
}
