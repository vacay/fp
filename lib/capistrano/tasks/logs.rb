task :stream_log do
  on roles(:fp), in: :parallel do
    execute "tail -f #{shared_path}/log/default.log"
  end
end

task :clean_logs do
  on roles(:fp), in: :parallel do
    execute "rm -rf #{shared_path}/log/*"
  end
end
