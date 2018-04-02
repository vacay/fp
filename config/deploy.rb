require_relative 'aws_creds'

# Set the location of your SSH key.  You can give a list of files, but
# the first key given will be the one used to upload your chef files to
# each server.
set :ssh_options, {
  :user => 'deploy', # overrides user setting above
  :forward_agent => true,
  :auth_methods => %w(publickey)
}

# Set the location of your cookbooks/data bags/roles for Chef
set :chef_cookbooks_path, 'kitchen/cookbooks'
set :chef_data_bags_path, 'kitchen/data_bags'
set :chef_roles_path, 'kitchen/roles'

set :application, 'vacay'
set :repo_url, 'git@github.com:vacay/fp.git'

set :deploy_to, '/home/deploy/vacay'
set :pty, true

# Default value for :linked_files is []
# set :linked_files, %w{config/database.yml}

# Default value for linked_dirs is []
# set :linked_dirs, %w{bin log tmp/pids tmp/cache tmp/sockets vendor/bundle public/system}

set :default_env, { 'NODE_ENV' => 'production' }
set :keep_releases, 2

set :use_sudo, true

namespace :deploy do
  after :updated, :npm_refresh_symlink
  after :updated, :npm_install

  desc 'Restart node script'
  after :publishing, :restart do
    invoke :forever_stop
    invoke :clean_logs
    invoke :forever_cleanlogs
    sleep 3
    invoke :forever_start
  end
end
