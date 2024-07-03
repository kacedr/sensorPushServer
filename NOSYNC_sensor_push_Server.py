import pyodbc
import time
import logging
import pandas as pd
import courier # notification api that will link with twilio 
import os

from warnings import filterwarnings
from dotenv import load_dotenv


###
# Adding the column "operational" to the SQL table will prevent the moving of all of the rows from table to table. 
# Currently, since every sensor is "offline" they are all moved into the offline_sensors table as its based on soley
# time since last heartbeat.
# Version without asyncio because asyncio is not the best, could still switch back to using an event loop
# todo: add cleanup in event of closure during sweeping, server should fix its self however 
###


# pyodbc works fine with pandas, ignoring warning 
filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

class SensorMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

        # Load environment variables from .env file
        load_dotenv()

        # Enable connection pooling to alleviate overhead on the db
        pyodbc.pooling = True

        # Create connection string
        self.connection_string = (
            f"DRIVER={os.getenv('DRIVER')};"
            f"SERVER={os.getenv('DB_HOST')};"
            f"DATABASE={os.getenv('DB_DATABASE')};"
            f"UID={os.getenv('DB_USERNAME')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
            f"charset={'utf8mb4'};"
        )

    def del_offline_sensors(self):
        """Function to delete sensors from the offline_sensors table that are back online"""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                
                # SQL query to delete entries when sensors are back online
                sql = """
                DELETE os
                FROM offline_sensors os
                JOIN sources s ON os.id_in_sources = s.id AND os.master_id = s.master_id
                WHERE TIMESTAMPDIFF(MINUTE, s.last_heartbeat, NOW()) < 30
                """
                # todo: above needs to be switched to "< 30" not ""> 3000", this is just for testing

                cursor.execute(sql)
                rows_deleted = cursor.rowcount
                conn.commit()  
                logging.info(f"{rows_deleted} entries deleted from offline_sensors based on sources data (sensor online).")
    
        except pyodbc.Error as e:
            logging.error(f"An error occurred while trying to delete sensor data: {e}")

    def add_offline_sensors_to_table(self):
        """Function to add/update sensors from the table that have undetected heartbeats"""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                
                # SQL query to join and update/insert data
                sql = """
                INSERT INTO offline_sensors (id_in_sources, service, master_id, master_name, last_heartbeat, minutes_since_last_heartbeat)
                SELECT s.id, s.service, s.master_id, s.master_name, s.last_heartbeat,
                    TIMESTAMPDIFF(MINUTE, s.last_heartbeat, NOW()) AS minutes_since_last_heartbeat
                FROM sources s
                WHERE TIMESTAMPDIFF(MINUTE, s.last_heartbeat, NOW()) > 30
                AND s.master_id IS NOT NULL
                ON DUPLICATE KEY UPDATE
                    service = VALUES(service),
                    master_id = VALUES(master_id),
                    master_name = VALUES(master_name),
                    last_heartbeat = VALUES(last_heartbeat),
                    minutes_since_last_heartbeat = VALUES(minutes_since_last_heartbeat);
                """
                
                cursor.execute(sql)
                conn.commit() 
                logging.info("Successfully updated offline_sensors table from sources.")
    
        except pyodbc.Error as e:
            logging.error(f"An error occurred while updating the database: {e}")

    def update_offline_sensors(self, df):
        """Update the offline_sensors table using direct SQL execution (caution: risk of SQL injection)."""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                for index, row in df.iterrows():
                    # SQL Injection is very possible here, since this is an internal tool however, this should be okay
                    # This is due to the sql driver not supporting batch updates
                    sql = f"""
                    UPDATE offline_sensors
                    SET 
                        notify_30m_primary = {int(row['notify_30m_primary'])},
                        notify_1hr_primary = {int(row['notify_1hr_primary'])},
                        notify_3hr_primary = {int(row['notify_3hr_primary'])},
                        notify_6hr_primary = {int(row['notify_6hr_primary'])},
                        notify_12hr_primary = {int(row['notify_12hr_primary'])},
                        notify_daily_primary = {int(row['notify_daily_primary'])},
                        notify_weekly_primary = {int(row['notify_weekly_primary'])},
                        notify_1hr_secondary = {int(row['notify_1hr_secondary'])},
                        notify_3hr_secondary = {int(row['notify_3hr_secondary'])},
                        notify_6hr_secondary = {int(row['notify_6hr_secondary'])},
                        notify_12hr_secondary = {int(row['notify_12hr_secondary'])},
                        notify_daily_secondary = {int(row['notify_daily_secondary'])},
                        notify_weekly_secondary = {int(row['notify_weekly_secondary'])}
                    WHERE id_in_sources = {int(row['id_in_sources'])} AND master_id = '{row['master_id']}';
                    """
                    cursor.execute(sql)
                conn.commit()
                logging.info("Successfully updated offline_sensors table from DataFrame.")
        except pyodbc.Error as e:
            conn.rollback()
            print(f"An error occurred during batch update: {e}")

    def check_and_notify(self, df):
        """Notifies stakeholders and updates table"""
        intervals = [
            (30, 60, 'notify_30m_primary', None),
            (60, 180, 'notify_1hr_primary', 'notify_1hr_secondary'),
            (180, 360, 'notify_3hr_primary', 'notify_3hr_secondary'),
            (360, 720, 'notify_6hr_primary', 'notify_6hr_secondary'),
            (720, 1440, 'notify_12hr_primary', 'notify_12hr_secondary'),
            (1440, 10080, 'notify_daily_primary', 'notify_daily_secondary'),
            (10080, float('inf'), 'notify_weekly_primary', 'notify_weekly_secondary')
        ]
        
        for start, end, primary_key, secondary_key in intervals:
            mask = (df['minutes_since_last_heartbeat'] > start) & (df['minutes_since_last_heartbeat'] <= end)
            
            for key in [primary_key, secondary_key]:
                if key:
                    if key.startswith('notify_daily'):
                        # Calculate the minimum minutes required for the next notification
                        min_minutes_for_next = 1440 * (df[key] + 1)
                        notify_mask = mask & (df[key] < 7) & (df['minutes_since_last_heartbeat'] >= min_minutes_for_next)
                    elif key.startswith('notify_weekly'):
                        # Calculate the minimum minutes required for the next notification
                        min_minutes_for_next = 10080 * (df[key] + 1)
                        notify_mask = mask & (df['minutes_since_last_heartbeat'] >= min_minutes_for_next)
                    else:
                        notify_mask = mask & (df[key] == 0)
                    
                    df.loc[notify_mask, key] += 1  # Increment the notification counter
                    for index in notify_mask[notify_mask].index:
                        sensor_id = df.at[index, 'id_in_sources']
                        logging.info(f"Notification sent for {key}: ID {sensor_id} at {df.at[index, 'last_heartbeat']}")
                        
                        # Send SMS and email notifications
                        self.send_sms(sensor_id)
                        self.send_email(sensor_id)
        return df

    def who_to_send_to(self):
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                sql = """
                SELECT id_in_sources, service, master_id, master_name, last_heartbeat, minutes_since_last_heartbeat,
                    notify_30m_primary, notify_1hr_primary, notify_3hr_primary, notify_6hr_primary, notify_12hr_primary, 
                    notify_daily_primary, notify_weekly_primary, notify_1hr_secondary, notify_3hr_secondary, 
                    notify_6hr_secondary, notify_12hr_secondary, notify_daily_secondary, notify_weekly_secondary 
                FROM offline_sensors;
                """
                logging.info("Successfully pulled from offline_sensors.")
                df = pd.read_sql(sql, conn)
                
                # Notify respected parties and get updated DataFrame
                updated_df = self.check_and_notify(df)
                
                # Update the offline_sensors table with the updated DataFrame
                self.update_offline_sensors(updated_df)

        except pyodbc.Error as e:
            logging.error(f"An error occurred while pulling from the database: {e}")


    def send_sms(self, id):
        """Function to send sms messages via api call"""
        # Search users table with id (can send in another identifer) and call api
        # Can not figure out what the link is between sensors in sources and their respected parties to notify.
        # is it master id? 
        pass
        
    def send_email(self, id):
        """Function to send email messages via api/smpt server call"""
        # Search users table with id (can send in another identifer) and call api
        pass
    
    def connect_and_execute(self):
        """Function to connect to the database and execute queries."""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                logging.info("Successfully connected to the database.")

                # add offline sensors to db
                self.add_offline_sensors_to_table() ## 1st sql call

                # remove online sensors from db
                self.del_offline_sensors() ## 2nd sql call

                # this is where we notify the stakeholders bassed off offline_sensors db
                self.who_to_send_to() ## 3rd, 4th, and 5th sql call
                
        except Exception as e:
            logging.error("An error occurred while connecting to the database: %s", e)
            
    def main_loop(self):
        """Main loop to run tasks every 5 minutes."""
        try:
            while True:
                logging.info("Starting the database operation...")
                self.connect_and_execute()
                logging.info("Waiting for the next cycle...")
                time.sleep(300)  # 5 minute sweep interval
        except KeyboardInterrupt:
            logging.info("Shutdown signal received. Exiting...")

            # close msg servers
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)

            # close msg servers

if __name__ == "__main__":
    monitor = SensorMonitor()
    monitor.main_loop()
