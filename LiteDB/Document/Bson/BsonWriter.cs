using System;
using System.Collections.Generic;
using System.Text;

namespace LiteDB
{
    /// <summary>
    /// Internal class to serialize a BsonDocument to BSON data format (byte[])
    /// </summary>
    internal struct BsonWriter
    {
        /// <summary>
        /// Main method - serialize document. Uses ByteWriter
        /// </summary>
        public byte[] Serialize(BsonDocument doc)
        {
            var count = doc.GetBytesCount(true);
            var writer = new ByteWriter(count);

            this.WriteDocument(ref writer, doc);

            return writer.Buffer;
        }

        /// <summary>
        /// Write a bson document
        /// </summary>
        public void WriteDocument(ref ByteWriter writer, BsonDocument doc)
        {
            writer.Write(doc.GetBytesCount(false));

            foreach (var key in doc.Keys)
            {
                this.WriteElement(ref writer, key, doc[key] ?? BsonValue.Null);
            }

            writer.Write((byte)0x00);
        }

        public void WriteArray(ref ByteWriter writer, BsonArray array)
        {
            writer.Write(array.GetBytesCount(false));

            for (var i = 0; i < array.Count; i++)
            {
                this.WriteElement(ref writer, IntToString(i), array[i] ?? BsonValue.Null);
            }

            writer.Write((byte)0x00);
        }

        private void WriteElement(ref ByteWriter writer, string key, BsonValue value)
        {
            // cast RawValue to avoid one if on As<Type>
            switch (value.Type)
            {
                case BsonType.Double:
                    writer.Write((byte)0x01);
                    writer.WriteCString(key);
                    writer.Write((Double)value.RawValue);
                    break;

                case BsonType.String:
                    writer.Write((byte)0x02);
                    writer.WriteCString(key);
                    this.WriteString(ref writer, (String)value.RawValue);
                    break;

                case BsonType.Document:
                    writer.Write((byte)0x03);
                    writer.WriteCString(key);
                    this.WriteDocument(ref writer, new BsonDocument((Dictionary<string, BsonValue>)value.RawValue));
                    break;

                case BsonType.Array:
                    writer.Write((byte)0x04);
                    writer.WriteCString(key);
                    this.WriteArray(ref writer, new BsonArray((List<BsonValue>)value.RawValue));
                    break;

                case BsonType.Binary:
                    writer.Write((byte)0x05);
                    writer.WriteCString(key);
                    var bytes = (byte[])value.RawValue;
                    writer.Write(bytes.Length);
                    writer.Write((byte)0x00); // subtype 00 - Generic binary subtype
                    writer.Write(bytes);
                    break;

                case BsonType.Guid:
                    writer.Write((byte)0x05);
                    writer.WriteCString(key);
                    var guid = ((Guid)value.RawValue).ToByteArray();
                    writer.Write(guid.Length);
                    writer.Write((byte)0x04); // UUID
                    writer.Write(guid);
                    break;

                case BsonType.ObjectId:
                    writer.Write((byte)0x07);
                    writer.WriteCString(key);
                    writer.Write(((ObjectId)value.RawValue).ToByteArray());
                    break;

                case BsonType.Boolean:
                    writer.Write((byte)0x08);
                    writer.WriteCString(key);
                    writer.Write((byte)(((Boolean)value.RawValue) ? 0x01 : 0x00));
                    break;

                case BsonType.DateTime:
                    writer.Write((byte)0x09);
                    writer.WriteCString(key);
                    var date = (DateTime)value.RawValue;
                    // do not convert to UTC min/max date values - #19
                    var utc = (date == DateTime.MinValue || date == DateTime.MaxValue) ? date : date.ToUniversalTime();
                    var ts = utc - BsonValue.UnixEpoch;
                    writer.Write(Convert.ToInt64(ts.TotalMilliseconds));
                    break;

                case BsonType.Null:
                    writer.Write((byte)0x0A);
                    writer.WriteCString(key);
                    break;

                case BsonType.Int32:
                    writer.Write((byte)0x10);
                    writer.WriteCString(key);
                    writer.Write((Int32)value.RawValue);
                    break;

                case BsonType.Int64:
                    writer.Write((byte)0x12);
                    writer.WriteCString(key);
                    writer.Write((Int64)value.RawValue);
                    break;

                case BsonType.Decimal:
                    writer.Write((byte)0x13);
                    writer.WriteCString(key);
                    writer.Write((Decimal)value.RawValue);
                    break;

                case BsonType.MinValue:
                    writer.Write((byte)0xFF);
                    writer.WriteCString(key);
                    break;

                case BsonType.MaxValue:
                    writer.Write((byte)0x7F);
                    writer.WriteCString(key);
                    break;
            }
        }

        private void WriteString(ref ByteWriter writer, string s)
        {
            var begin = writer.Position;
            writer.Position += 4;
            var len = writer.WriteJustString(s);
            writer.Write((byte)0x00); len += 1;

            writer.Write((uint)len, begin);
        }

        private void WriteCString(ref ByteWriter writer, string s)
        {
            writer.WriteCString(s);
        }

        static string[] IntStringCache = new string[16];

        static BsonWriter()
        {
            for (int i = 0; i < IntStringCache.Length; i++) {
                IntStringCache[i] = i.ToString();
            }
        }

        static internal string IntToString(int i)
        {
            if (i < IntStringCache.Length)
                return IntStringCache[i];
            return i.ToString();
        }
    }
}