namespace :forever do
  desc 'Install forever globally'
  task :setup do
    on roles(:fp), in: :parallel do
      execute "sudo npm install -g forever"
    end
  end
end

desc 'Stop node script'
task :forever_stop do
  on roles(:fp), in: :parallel do
    execute "sudo forever stopall --killSignal=SIGTERM; true"
  end
end

task :forever_start do
  on roles(:fp), in: :parallel do |host|
    execute "sudo NODE_ENV=production forever start -s #{release_path}/app.js"
  end
end

task :forever_cleanlogs do
  on roles(:fp), in: :parallel do
    execute "sudo forever cleanlogs"
  end
end
