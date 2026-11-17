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

pkgrel=1
# makepkg's lint step needs a non-empty pkgver, so give it a placeholder that
# update_pkgver() will replace with the value returned from pkgver()
pkgver=0.1.0.r0.g

pkgver() {
  # mirror Cargo.toml's version (TOML: `version = "0.1.0"` -- the spaces matter) and
  # append the git commit so every upstream change yields a new version. makepkg calls
  # pkgver() after the sources are extracted, with its cwd set to $srcdir — which does
  # not carry .git (the repo is at $srcdir/signal-tauri/), so point at it explicitly.
  # When a build host sources this PKGBUILD directly (from a plain clone), the repo is
  # right here at the root, and Cargo.toml sits at the root too.
  local ver=""
  if [ -f "Cargo.toml" ]; then
    ver="$(grep -m1 '^version *=' Cargo.toml | sed 's/^[^=]*= *//' | tr -d ' "')"
  elif [ -f "$srcdir/signal-tauri/Cargo.toml" ]; then
    ver="$(grep -m1 '^version *=' "$srcdir/signal-tauri/Cargo.toml" | sed 's/^[^=]*= *//' | tr -d ' "')"
  else
    ver="0.1.0"
  fi
  # makepkg runs with cwd=$srcdir (no .git); a build host sources from the clone root (.git).
  git_dir="$([ -d "$srcdir/signal-tauri/.git" ] && printf '%s' "$srcdir/signal-tauri/.git" || printf '%s' ".git")"
  printf '%s.r%s.g%s' "${ver:-0.1.0}" "$(git --git-dir "$git_dir" rev-list --count HEAD)" \
      "$(git --git-dir "$git_dir" rev-parse --short HEAD)"
}

package() {
  # the container (archlinux:base-devel, --network host) resolves crates.io and the
  # presage git deps from github.com; cargo + rustc do their heavy lifting there.
  cargo build --release
  install -Dm755 target/release/signal-tauri "$pkgdir/usr/bin/signal-tauri"
}
