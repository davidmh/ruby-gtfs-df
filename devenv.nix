{ pkgs, lib, config, inputs, ... }:

{
  languages.ruby = {
    enable = true;
    version = "3.4.7";
    bundler.enable = false;
  };

  git-hooks.hooks = {
    conform.enable = true;
  };

  packages = with pkgs; [ git-cliff ];
}
