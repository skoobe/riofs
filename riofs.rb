require 'formula'

class Riofs < Formula
  homepage 'https://github.com/skoobe/riofs'
  url 'https://github.com/skoobe/riofs.git'
  version '0.6'

  depends_on 'autoconf' => :build
  depends_on 'automake' => :build
  depends_on 'pkg-config' => :build
  depends_on 'glib'
  depends_on 'osxfuse'
  depends_on 'libevent'
  depends_on 'libmagic' => :recommended
  depends_on 'openssl' => :recommended

  def install
    system "./autogen.sh"
    system "./configure --prefix=#{prefix}"
    system "make install"
  end
end
