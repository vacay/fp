task :npm_refresh_symlink do
  on roles(:fp), in: :parallel do
    execute "rm -rf #{release_path}/node_modules && ln -s #{shared_path}/node_modules/ #{release_path}/node_modules"
  end
end

task :npm_install do
  on roles(:fp), in: :parallel do
    execute "cd #{release_path}/ && sudo npm install --production --loglevel silent --unsafe-perm"
  end
end
