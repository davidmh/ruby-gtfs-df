{ pkgs, ... }:

{
  devenv.warnOnNewVersion = false;

  languages.ruby = {
    enable = true;
    versionFile = ./.ruby-version;
    bundler.enable = false;
  };

  git-hooks.hooks = {
    conform.enable = true;
  };

  packages = with pkgs; [ git-cliff ];
}
