require "spec_helper"

RSpec.describe PgHaMigrations::BlockingDatabaseTransactions do
  it "ignores streaming replication connections" do
    ActiveRecord::Base.run_sql <<~SQL
      SELECT pg_create_logical_replication_slot('pg_ha_migrations_test_slot', 'test_decoding');
    SQL

    begin
      thread_errors = Queue.new

      thread = Thread.new do
        begin
          database_name = ActiveRecord::Base.connection.current_database
          system "pg_recvlogical --slot=pg_ha_migrations_test_slot --start --no-loop -d #{database_name} --file - 2>&1 > /dev/null"
        rescue => e
          thread_errors << e
        end
      end

      i = 0
      replication_connections = 0
      while thread_errors.empty? && replication_connections.zero?
        Thread.pass
        sleep 0.5
        replication_connections = ActiveRecord::Base.value_from_sql <<~SQL
          SELECT COUNT(*) FROM pg_stat_activity WHERE backend_type = 'walsender'
        SQL
        raise "Timed out waiting for replication connection" if (i += 1) > 15
      end

      begin
        expect(PgHaMigrations::BlockingDatabaseTransactions.find_blocking_transactions).to be_empty
      ensure
        ActiveRecord::Base.run_sql <<~SQL
          SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'pg_recvlogical'
        SQL
        thread.kill
        thread.join
      end

      unless thread_errors.empty?
        raise thread_errors.pop
      end
    ensure
      ActiveRecord::Base.run_sql("SELECT pg_drop_replication_slot('pg_ha_migrations_test_slot')")
    end
  end
end
