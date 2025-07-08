import sqlite3
import json
import pandas as pd
from datetime import datetime
import os

class JSONMessageDB:
    """SQLite database handler for JSON messages"""
    
    def __init__(self, db_path="json_messages.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database and create tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS json_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                serial_no INTEGER,
                timestamp TEXT,
                topic TEXT,
                json_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index for better query performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON json_messages(timestamp)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_topic ON json_messages(topic)
        ''')
        
        conn.commit()
        conn.close()
    
    def insert_message(self, serial_no, timestamp, topic, json_data):
        """Insert a single JSON message into the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO json_messages (serial_no, timestamp, topic, json_data)
            VALUES (?, ?, ?, ?)
        ''', (serial_no, timestamp, topic, json.dumps(json_data)))
        
        conn.commit()
        conn.close()
    
    def insert_messages_batch(self, messages):
        """Insert multiple JSON messages in batch"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        data_to_insert = []
        for msg in messages:
            data_to_insert.append((
                msg.get('Serial No.', 0),
                msg.get('Timestamp', ''),
                msg.get('Topic', ''),
                json.dumps(msg.get('JSON Data', {}))
            ))
        
        cursor.executemany('''
            INSERT INTO json_messages (serial_no, timestamp, topic, json_data)
            VALUES (?, ?, ?, ?)
        ''', data_to_insert)
        
        conn.commit()
        conn.close()
    
    def get_all_messages(self):
        """Retrieve all JSON messages from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT serial_no, timestamp, topic, json_data, created_at
            FROM json_messages
            ORDER BY created_at DESC
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        messages = []
        for row in rows:
            try:
                json_data = json.loads(row[3])
            except json.JSONDecodeError:
                json_data = {}
            
            messages.append({
                'Serial No.': row[0],
                'Timestamp': row[1],
                'Topic': row[2],
                'JSON Data': json_data,
                'Created At': row[4]
            })
        
        return messages
    
    def get_messages_by_topic(self, topic):
        """Retrieve messages filtered by topic"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT serial_no, timestamp, topic, json_data, created_at
            FROM json_messages
            WHERE topic = ?
            ORDER BY created_at DESC
        ''', (topic,))
        
        rows = cursor.fetchall()
        conn.close()
        
        messages = []
        for row in rows:
            try:
                json_data = json.loads(row[3])
            except json.JSONDecodeError:
                json_data = {}
            
            messages.append({
                'Serial No.': row[0],
                'Timestamp': row[1],
                'Topic': row[2],
                'JSON Data': json_data,
                'Created At': row[4]
            })
        
        return messages
    
    def clear_all_messages(self):
        """Clear all messages from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM json_messages')
        conn.commit()
        conn.close()
    
    def get_message_count(self):
        """Get total count of messages in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM json_messages')
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    
    def get_topics(self):
        """Get list of unique topics in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT topic FROM json_messages ORDER BY topic')
        topics = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return topics
    
    def delete_database(self):
        """Delete the entire database file"""
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                return True
        except Exception as e:
            print(f"Error deleting database: {e}")
            return False
        return False