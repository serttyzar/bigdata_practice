import asyncio
from telethon import TelegramClient, events
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime
from collections import defaultdict

API_ID = '---'
API_HASH = '---'

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'telegram_messages'

CHANNEL_IDS = [
    'rian_ru',
    'bbcrussian',
    'meduzalive',
    'rt_russian',
    'lentach',
    'tass_agency',
    'rbc_news',
    'https://t.me/+IyYSM7u2i7E4MTgy',
]


class TelegramKafkaProducer:
    def __init__(self, api_id, api_hash, kafka_servers, kafka_topic):
        self.client = TelegramClient('session', api_id, api_hash)
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic
        self.channels = {}
    
    async def join_channels(self, channel_list):
        for item in channel_list:
            try:
                entity = None
                
                if isinstance(item, int):
                    entity = await self.client.get_entity(item)
                    
                elif item.startswith('https://t.me/+') or item.startswith('https://t.me/joinchat/'):
                    invite_hash = item.split('joinchat/')[-1] if 'joinchat/' in item else item.split('+')[-1]
                    
                    try:
                        result = await self.client(CheckChatInviteRequest(invite_hash))
                        
                        if hasattr(result, 'chat'):
                            entity = await self.client.get_entity(result.chat)
                        else:
                            result = await self.client(ImportChatInviteRequest(invite_hash))
                            if hasattr(result, 'chats') and result.chats:
                                entity = await self.client.get_entity(result.chats[0].id)
                    
                    except Exception as e:
                        if "already a participant" in str(e):
                            dialogs = await self.client.get_dialogs()
                            for dialog in dialogs:
                                if dialog.is_group or dialog.is_channel:
                                    entity = dialog.entity
                                    break
                    
                else:
                    if not item.startswith('@'):
                        item = '@' + item
                    entity = await self.client.get_entity(item)
                
                if entity:
                    self.channels[entity.id] = entity.title
                    print(f"Connected: {entity.title}")
                
            except Exception as e:
                print(f"Error connecting to {item}: {e}")
    
    async def handle_new_message(self, event):
        message = event.message
        sender = await event.get_sender()
        
        if sender:
            username = (getattr(sender, 'username', None) or
                       getattr(sender, 'first_name', None) or
                       f"user_{sender.id}")
        else:
            username = "unknown"
        
        message_data = {
            'username': username,
            'channel_id': event.chat_id,
            'message_id': message.id,
            'text': message.text or '',
            'timestamp': int(message.date.timestamp())
        }
        
        self.kafka_producer.send(self.kafka_topic, value=message_data)
        print(f"Message from {username}")
    
    async def start_listening(self):
        @self.client.on(events.NewMessage(chats=list(self.channels.keys())))
        async def handler(event):
            await self.handle_new_message(event)
        
        await self.client.run_until_disconnected()
    
    async def run(self, channel_ids):
        await self.client.start()
        await self.join_channels(channel_ids)
        await self.start_listening()
    
    def close(self):
        self.kafka_producer.close()


class MessageCounter:
    def __init__(self, kafka_servers, kafka_topic):
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.messages = defaultdict(list)
        self.last_print_time = time.time()
    
    def count_messages_in_window(self, window_minutes):
        current_time = time.time()
        cutoff_time = current_time - (window_minutes * 60)
        
        counts = {}
        for username, messages in self.messages.items():
            count = sum(1 for timestamp, _ in messages if timestamp >= cutoff_time)
            if count > 0:
                counts[username] = count
        
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)
    
    def print_statistics(self):
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}]")
        
        print("\n1 minute window:")
        stats_1min = self.count_messages_in_window(1)
        for username, count in stats_1min:
            print(f"{username}, {count}")
        
        print("\n10 minutes window:")
        stats_10min = self.count_messages_in_window(10)
        for username, count in stats_10min:
            print(f"{username}, {count}")
        print()
    
    def cleanup_old_messages(self):
        current_time = time.time()
        cutoff_time = current_time - (10 * 60)
        
        for username in list(self.messages.keys()):
            self.messages[username] = [
                (ts, msg_id) for ts, msg_id in self.messages[username] 
                if ts >= cutoff_time
            ]
            if not self.messages[username]:
                del self.messages[username]
    
    def run(self):
        try:
            for message in self.consumer:
                data = message.value
                username = data['username']
                timestamp = data['timestamp']
                message_id = data['message_id']
                
                self.messages[username].append((timestamp, message_id))
                
                current_time = time.time()
                if current_time - self.last_print_time >= 30:
                    self.print_statistics()
                    self.cleanup_old_messages()
                    self.last_print_time = current_time
                    
        except KeyboardInterrupt:
            self.print_statistics()
        finally:
            self.consumer.close()


def run_producer():
    producer = TelegramKafkaProducer(API_ID, API_HASH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    
    try:
        with producer.client:
            producer.client.loop.run_until_complete(producer.run(CHANNEL_IDS))
    finally:
        producer.close()


def run_consumer():
    counter = MessageCounter(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    counter.run()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python hw3.py [producer|consumer]")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == "producer":
        run_producer()
    elif mode == "consumer":
        run_consumer()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)