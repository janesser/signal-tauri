# Maintainer: jan <jan@...>
echo "I_AM_SOURCED" >> .sourced.log
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

pkgrel=1

pkgver() {
  printf 'DBG: called pkgver cwd=[%s] srcdir=[%s]\n' "$(pwd)" "$srcdir" >> /builds/signal-tauri-bin/.called.log
  echo "0.1.0"
}

# check whether makepkg sees pkgver as a function
if declare -f pkgver >/dev/null 2>&1; then
  printf 'DBG: declare -f sees function\n' >> .called.log
else
  printf 'DBG: declare -f does NOT see function\n' >> .called.log
fi

package() {
  # the container (archlinux:base-devel, --network host) resolves crates.io and the
  # presage git deps from github.com; cargo + rustc do their heavy lifting there.
  cargo build --release
  install -Dm755 target/release/signal-tauri "$pkgdir/usr/bin/signal-tauri"
}
