# Decide where you want your home-manager config to live
HM_DIR=$HOME/my-home-manager

# Download and unpack the home-manager config template
cd /tmp
curl -L https://github.com/jacg/IC/tarball/nix-trials > IC.tgz
nix-shell -p pkgs.gnutar --run "tar xvf IC.tgz --wildcards '*/nix/home-manager' --strip-components=3"
mv home-manager $HM_DIR

# Bootstrap your personal home-manager installation and configuration
cd $HM_DIR
nix-shell bootstrap-home-manager
# You should also run `nix-shell bootstrap-home-manager` (from $HM_DIR) whenever
# you change the home-manager version in ./sources.nix

# Home-manager is ready: after changing your `home.nix` you should be able to
# switch to the newly-specified configuration by running `home-manager switch`
# from any directory:

# home-manager -b bck switch

# the `-b bck` option is useful if you ask home-manager to create links (to
# configuration files) in your home directory. If it encounters pre-existing
# versions of such files, it will back them up to <original name>.bck.
