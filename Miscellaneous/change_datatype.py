from sqlalchemy import text



sql_statement = text("ALTER TABLE orders_table,
  ALTER COLUMN date_uuid SET DATA TYPE UUID,
  ALTER COLUMN user_uuid SET DATA TYPE UUID,
  ALTER COLUMN card_number SET DATA TYPE VARCHAR(16),
  ALTER COLUMN store_code SET DATA TYPE VARCHAR(10), 
  ALTER COLUMN product_code SET DATA TYPE VARCHAR(10), 
  ALTER COLUMN product_quantity SET DATA TYPE SMALLINT;")
with self.db_engine.connect() as connection:
    connection.execute(sql_statement)