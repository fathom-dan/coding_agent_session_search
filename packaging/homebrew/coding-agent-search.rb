class Cass < Formula
  desc "Cross-agent session search for AI coding conversations"
  homepage "https://github.com/Dicklesworthstone/coding_agent_session_search"
  version "0.2.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-darwin-arm64.tar.gz"
      sha256 "bdf7b7ff0374317ad3286d95ed767e59974626fc964b3a9ff8db25e7f5bdc367"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-linux-amd64.tar.gz"
      sha256 "32ccb596de7e72b31f186f3b2fb14764386e4606bb976585ecc4f0db3dffaffb"
    end
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-linux-arm64.tar.gz"
      sha256 "11ec6b728311a385158df4f0bf8913fd59654ef8e2205df9d3c9219fcbadb25a"
    end
  end

  def install
    bin.install "cass"
    generate_completions_from_executable(bin/"cass", "completions")
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/cass --version")
    assert_match "health", shell_output("#{bin}/cass --help")
  end
end
