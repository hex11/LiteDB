using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace LiteDB
{
    /// <summary>
    /// Represent a Bson Value used in BsonDocument
    /// </summary>
    public class BsonValue : IComparable<BsonValue>, IEquatable<BsonValue>
    {
        public static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Represent a Null bson type
        /// </summary>
        public static readonly BsonValue Null = new BsonValue();

        /// <summary>
        /// Represent a MinValue bson type
        /// </summary>
        public static readonly BsonValue MinValue = new BsonValue { Type = BsonType.MinValue, RawValue = "-oo" };

        /// <summary>
        /// Represent a MaxValue bson type
        /// </summary>
        public static readonly BsonValue MaxValue = new BsonValue { Type = BsonType.MaxValue, RawValue = "+oo" };

        /// <summary>
        /// Indicate BsonType of this BsonValue
        /// </summary>
        public BsonType Type { get; private set; }

        /// <summary>
        /// Get internal .NET value object
        /// </summary>
        public virtual object RawValue
        {
            get {
                if (raw != null)
                    return raw;
                if (Type == BsonType.Null)
                    return null;

                // then it's a inplace value, which must be returned as a boxed value.
                object boxedValue = null;
                if (Type == BsonType.Int32)
                    boxedValue = (int)inplace;
                if (Type == BsonType.Int64)
                    boxedValue = inplace;
                if (Type == BsonType.Boolean)
                    boxedValue = inplace != 0;

                // (should we use raw to cache the object?)
                raw = boxedValue;
                return boxedValue;
            }
            private set {
                raw = value;
            }
        }

        object raw;

        // use this field to store some value types to avoid boxing in some cases
        long inplace;

        /// <summary>
        /// Remove this value from parent
        /// </summary>
        internal void Destroy()
        {
            if (parent is BsonDocument doc) {
                doc.Remove(nameInParent);
            } else if (parent is BsonArray arr) {
                arr.Remove(this);
            }
        }

        internal BsonValue parent;
        internal string nameInParent;

        #region Constructor

        public BsonValue()
        {
            this.Type = BsonType.Null;
            this.RawValue = null;
        }

        public BsonValue(Int32 value)
        {
            this.Type = BsonType.Int32;
            this.inplace = value;
        }

        public BsonValue(Int64 value)
        {
            this.Type = BsonType.Int64;
            this.inplace = value;
        }

        public BsonValue(Double value)
        {
            this.Type = BsonType.Double;
            this.RawValue = value;
        }

        public BsonValue(Decimal value)
        {
            this.Type = BsonType.Decimal;
            this.RawValue = value;
        }

        public BsonValue(String value)
        {
            this.Type = value == null ? BsonType.Null : BsonType.String;
            this.RawValue = value;
        }

        public BsonValue(Dictionary<string, BsonValue> value)
        {
            this.Type = value == null ? BsonType.Null : BsonType.Document;
            this.RawValue = value;
        }

        public BsonValue(List<BsonValue> value)
        {
            this.Type = value == null ? BsonType.Null : BsonType.Array;
            this.RawValue = value;
        }

        public BsonValue(Byte[] value)
        {
            this.Type = value == null ? BsonType.Null : BsonType.Binary;
            this.RawValue = value;
        }

        public BsonValue(ObjectId value)
        {
            this.Type = value == null ? BsonType.Null : BsonType.ObjectId;
            this.RawValue = value;
        }

        public BsonValue(Guid value)
        {
            this.Type = BsonType.Guid;
            this.RawValue = value;
        }

        public BsonValue(Boolean value)
        {
            this.Type = BsonType.Boolean;
            this.inplace = value ? 1 : 0;
        }

        public BsonValue(DateTime value)
        {
            this.Type = BsonType.DateTime;
            this.RawValue = value.Truncate();
        }

        public BsonValue(BsonValue value)
        {
            this.Type = value == null ? BsonType.Null : value.Type;
            this.RawValue = value.RawValue;
        }

        public BsonValue(object value)
        {
            if (value == null) {
                this.Type = BsonType.Null;
            } else if (value is Int32 i32) {
                this.Type = BsonType.Int32;
                this.inplace = i32;
            } else if (value is Int64 i64) {
                this.Type = BsonType.Int64;
                this.inplace = i64;
            } else if (value is Boolean b) {
                this.Type = BsonType.Boolean;
                this.inplace = b ? 1 : 0;
            } else {
                this.RawValue = value;
                if (value is Double) this.Type = BsonType.Double;
                else if (value is Decimal) this.Type = BsonType.Decimal;
                else if (value is String) this.Type = BsonType.String;
                else if (value is Dictionary<string, BsonValue>) this.Type = BsonType.Document;
                else if (value is List<BsonValue>) this.Type = BsonType.Array;
                else if (value is Byte[]) this.Type = BsonType.Binary;
                else if (value is ObjectId) this.Type = BsonType.ObjectId;
                else if (value is Guid) this.Type = BsonType.Guid;
                else if (value is DateTime) {
                    this.Type = BsonType.DateTime;
                    this.RawValue = ((DateTime)value).Truncate();
                } else if (value is BsonValue v) {
                    this.Type = v.Type;
                    this.raw = v.raw;
                    this.inplace = v.inplace;
                } else {
                    // test first for dictionary (because IDictionary implements IEnumerable)
                    if (value is System.Collections.IDictionary dictionary) {
                        var dict = new Dictionary<string, BsonValue>();

                        foreach (var key in dictionary.Keys) {
                            dict.Add(key.ToString(), new BsonValue(dictionary[key]));
                        }

                        this.Type = BsonType.Document;
                        this.RawValue = dict;
                    } else if (value is System.Collections.IEnumerable enumerable) {
                        var list = new List<BsonValue>();
                        if (value is System.Collections.ICollection coll)
                            list.Capacity = coll.Count;

                        foreach (var x in enumerable) {
                            list.Add(new BsonValue(x));
                        }

                        this.Type = BsonType.Array;
                        this.RawValue = list;
                    } else {
                        throw new InvalidCastException("Value is not a valid BSON data type - Use Mapper.ToDocument for more complex types converts");
                    }
                }
            }
        }

        #endregion

        #region Convert types

        public BsonArray AsArray
        {
            get {
                if (this is BsonArray arr) return arr;
                if (IsArray) {
                    arr = new BsonArray((List<BsonValue>)RawValue);
                    arr.Length = this.Length;
                    arr.parent = this.parent;
                    arr.nameInParent = this.nameInParent;
                    return arr;
                } else {
                    return null;
                }
            }
        }

        public BsonDocument AsDocument
        {
            get {
                if (this is BsonDocument doc) return doc;
                if (IsDocument) {
                    doc = new BsonDocument((Dictionary<string, BsonValue>)RawValue);
                    doc.Length = this.Length;
                    doc.parent = this.parent;
                    doc.nameInParent = this.nameInParent;
                    return doc;
                } else {
                    return null;
                }
            }
        }

        public Byte[] AsBinary
        {
            get { return this.Type == BsonType.Binary ? (Byte[])this.RawValue : default(Byte[]); }
        }

        public bool AsBoolean
        {
            get { return inplace != 0; }
        }

        public string AsString
        {
            get { return this.RawValue?.ToString(); }
        }

        public int AsInt32
        {
            get {
                if (IsInplace) return (int)inplace;
                return this.IsNumber ? Convert.ToInt32(this.RawValue) : default(Int32);
            }
        }

        public long AsInt64
        {
            get {
                if (IsInplace) return inplace;
                return this.IsNumber ? Convert.ToInt64(this.RawValue) : default(Int64);
            }
        }

        public double AsDouble
        {
            get {
                if (IsInplace) return inplace;
                return this.IsNumber ? Convert.ToDouble(this.RawValue) : default(Double);
            }
        }

        public decimal AsDecimal
        {
            get {
                if (IsInplace) return inplace;
                return this.IsNumber ? Convert.ToDecimal(this.RawValue) : default(Decimal);
            }
        }

        public DateTime AsDateTime
        {
            get { return this.Type == BsonType.DateTime ? (DateTime)this.RawValue : default(DateTime); }
        }

        public ObjectId AsObjectId
        {
            get { return this.Type == BsonType.ObjectId ? (ObjectId)this.RawValue : default(ObjectId); }
        }

        public Guid AsGuid
        {
            get { return this.Type == BsonType.Guid ? (Guid)this.RawValue : default(Guid); }
        }

        #endregion

        #region IsTypes

        public bool IsNull
        {
            get { return this.Type == BsonType.Null; }
        }

        public bool IsArray
        {
            get { return this.Type == BsonType.Array; }
        }

        public bool IsDocument
        {
            get { return this.Type == BsonType.Document; }
        }

        public bool IsInt32
        {
            get { return this.Type == BsonType.Int32; }
        }

        public bool IsInt64
        {
            get { return this.Type == BsonType.Int64; }
        }

        public bool IsDouble
        {
            get { return this.Type == BsonType.Double; }
        }

        public bool IsDecimal
        {
            get { return this.Type == BsonType.Decimal; }
        }

        public bool IsNumber
        {
            get { return this.IsInt32 || this.IsInt64 || this.IsDouble || this.IsDecimal; }
        }

        public bool IsBinary
        {
            get { return this.Type == BsonType.Binary; }
        }

        public bool IsBoolean
        {
            get { return this.Type == BsonType.Boolean; }
        }

        public bool IsString
        {
            get { return this.Type == BsonType.String; }
        }

        public bool IsObjectId
        {
            get { return this.Type == BsonType.ObjectId; }
        }

        public bool IsGuid
        {
            get { return this.Type == BsonType.Guid; }
        }

        public bool IsDateTime
        {
            get { return this.Type == BsonType.DateTime; }
        }

        public bool IsMinValue
        {
            get { return this.Type == BsonType.MinValue; }
        }

        public bool IsMaxValue
        {
            get { return this.Type == BsonType.MaxValue; }
        }

        bool IsInplace
        {
            get {
                return inplace != 0 || IsInt32 || IsInt64 || IsBoolean;
            }
        }

        #endregion

        #region Implicit Ctor

        // Int32
        public static implicit operator Int32(BsonValue value)
        {
            return value.AsInt32;
        }

        // Int32
        public static implicit operator BsonValue(Int32 value)
        {
            return new BsonValue(value);
        }

        // Int64
        public static implicit operator Int64(BsonValue value)
        {
            return value.AsInt64;
        }

        // Int64
        public static implicit operator BsonValue(Int64 value)
        {
            return new BsonValue(value);
        }

        // Double
        public static implicit operator Double(BsonValue value)
        {
            return (Double)value.RawValue;
        }

        // Double
        public static implicit operator BsonValue(Double value)
        {
            return new BsonValue(value);
        }

        // Decimal
        public static implicit operator Decimal(BsonValue value)
        {
            return (Decimal)value.RawValue;
        }

        // Decimal
        public static implicit operator BsonValue(Decimal value)
        {
            return new BsonValue(value);
        }

        // UInt64 (to avoid ambigous between Double-Decimal)
        public static implicit operator UInt64(BsonValue value)
        {
            return (UInt64)value.RawValue;
        }

        // Decimal
        public static implicit operator BsonValue(UInt64 value)
        {
            return new BsonValue((Double)value);
        }

        // String
        public static implicit operator String(BsonValue value)
        {
            return (String)value.RawValue;
        }

        // String
        public static implicit operator BsonValue(String value)
        {
            return new BsonValue(value);
        }

        // Document
        public static implicit operator Dictionary<string, BsonValue>(BsonValue value)
        {
            return (Dictionary<string, BsonValue>)value.RawValue;
        }

        // Document
        public static implicit operator BsonValue(Dictionary<string, BsonValue> value)
        {
            return new BsonValue(value);
        }

        // Array
        public static implicit operator List<BsonValue>(BsonValue value)
        {
            return (List<BsonValue>)value.RawValue;
        }

        // Array
        public static implicit operator BsonValue(List<BsonValue> value)
        {
            return new BsonValue(value);
        }

        // Binary
        public static implicit operator Byte[] (BsonValue value)
        {
            return (Byte[])value.RawValue;
        }

        // Binary
        public static implicit operator BsonValue(Byte[] value)
        {
            return new BsonValue(value);
        }

        // ObjectId
        public static implicit operator ObjectId(BsonValue value)
        {
            return (ObjectId)value.RawValue;
        }

        // ObjectId
        public static implicit operator BsonValue(ObjectId value)
        {
            return new BsonValue(value);
        }

        // Guid
        public static implicit operator Guid(BsonValue value)
        {
            return (Guid)value.RawValue;
        }

        // Guid
        public static implicit operator BsonValue(Guid value)
        {
            return new BsonValue(value);
        }

        // Boolean
        public static implicit operator Boolean(BsonValue value)
        {
            return value.AsBoolean;
        }

        // Boolean
        public static implicit operator BsonValue(Boolean value)
        {
            return new BsonValue(value);
        }

        // DateTime
        public static implicit operator DateTime(BsonValue value)
        {
            return (DateTime)value.RawValue;
        }

        // DateTime
        public static implicit operator BsonValue(DateTime value)
        {
            return new BsonValue(value);
        }

        // +
        public static BsonValue operator +(BsonValue left, BsonValue right)
        {
            if (!left.IsNumber || !right.IsNumber) return BsonValue.Null;

            var type = (BsonType)Math.Max((int)left.Type, (int)right.Type);

            if (type == BsonType.Int32) return left.AsInt32 + right.AsInt32;
            if (type == BsonType.Int64) return left.AsInt64 + right.AsInt64;
            if (type == BsonType.Double) return left.AsDouble + right.AsDouble;
            return left.AsDecimal + right.AsDecimal;
        }

        // -
        public static BsonValue operator -(BsonValue left, BsonValue right)
        {
            if (!left.IsNumber || !right.IsNumber) return BsonValue.Null;

            var type = (BsonType)Math.Max((int)left.Type, (int)right.Type);

            if (type == BsonType.Int32) return left.AsInt32 - right.AsInt32;
            if (type == BsonType.Int64) return left.AsInt64 - right.AsInt64;
            if (type == BsonType.Double) return left.AsDouble - right.AsDouble;
            return left.AsDecimal - right.AsDecimal;
        }

        // *
        public static BsonValue operator *(BsonValue left, BsonValue right)
        {
            if (!left.IsNumber || !right.IsNumber) return BsonValue.Null;

            var type = (BsonType)Math.Max((int)left.Type, (int)right.Type);

            if (type == BsonType.Int32) return left.AsInt32 * right.AsInt32;
            if (type == BsonType.Int64) return left.AsInt64 * right.AsInt64;
            if (type == BsonType.Double) return left.AsDouble * right.AsDouble;
            return left.AsDecimal * right.AsDecimal;
        }

        // /
        public static BsonValue operator /(BsonValue left, BsonValue right)
        {
            if (!left.IsNumber || !right.IsNumber) return BsonValue.Null;
            if (left.IsDecimal || right.IsDecimal) return left.AsDecimal / right.AsDecimal;

            return left.AsDouble / right.AsDouble;
        }

        public override string ToString()
        {
            return JsonSerializer.Serialize(this);
        }

        #endregion

        #region IComparable<BsonValue>, IEquatable<BsonValue>

        public virtual int CompareTo(BsonValue other)
        {
            // first, test if types are different
            if (this.Type != other.Type)
            {
                // if both values are number, convert them to Decimal (128 bits) to compare
                // it's the slowest way, but more secure
                if (this.IsNumber && other.IsNumber)
                {
                    return Convert.ToDecimal(this.RawValue).CompareTo(Convert.ToDecimal(other.RawValue));
                }
                // if not, order by sort type order
                else
                {
                    return this.Type.CompareTo(other.Type);
                }
            }

            // for both values with same data type just compare
            switch (this.Type)
            {
                case BsonType.Null:
                case BsonType.MinValue:
                case BsonType.MaxValue:
                    return 0;

                case BsonType.Int32: return this.AsInt32.CompareTo(other.AsInt32);
                case BsonType.Int64: return this.AsInt64.CompareTo(other.AsInt64);
                case BsonType.Double: return ((Double)this.RawValue).CompareTo((Double)other.RawValue);
                case BsonType.Decimal: return ((Decimal)this.RawValue).CompareTo((Decimal)other.RawValue);

                case BsonType.String: return string.Compare((String)this.RawValue, (String)other.RawValue);

                case BsonType.Document: return this.AsDocument.CompareTo(other);
                case BsonType.Array: return this.AsArray.CompareTo(other);

                case BsonType.Binary: return ((Byte[])this.RawValue).BinaryCompareTo((Byte[])other.RawValue);
                case BsonType.ObjectId: return ((ObjectId)this.RawValue).CompareTo((ObjectId)other.RawValue);
                case BsonType.Guid: return ((Guid)this.RawValue).CompareTo((Guid)other.RawValue);

                case BsonType.Boolean: return this.AsBoolean.CompareTo(other.AsBoolean);
                case BsonType.DateTime:
                    var d0 = (DateTime)this.RawValue;
                    var d1 = (DateTime)other.RawValue;
                    if (d0.Kind != DateTimeKind.Utc) d0 = d0.ToUniversalTime();
                    if (d1.Kind != DateTimeKind.Utc) d1 = d1.ToUniversalTime();
                    return d0.CompareTo(d1);

                default: throw new NotImplementedException();
            }
        }

        public bool Equals(BsonValue other)
        {
            return this.CompareTo(other) == 0;
        }

        #endregion

        #region Operators

        public static bool operator ==(BsonValue lhs, BsonValue rhs)
        {
            if (object.ReferenceEquals(lhs, null)) return object.ReferenceEquals(rhs, null);
            if (object.ReferenceEquals(rhs, null)) return false; // don't check type because sometimes different types can be ==

            return lhs.Equals(rhs);
        }

        public static bool operator !=(BsonValue lhs, BsonValue rhs)
        {
            return !(lhs == rhs);
        }

        public static bool operator >=(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) >= 0;
        }

        public static bool operator >(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) > 0;
        }

        public static bool operator <(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) < 0;
        }

        public static bool operator <=(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) <= 0;
        }

        public override bool Equals(object obj)
        {
            return this.Equals(new BsonValue(obj));
        }

        public override int GetHashCode()
        {
            var hash = 17;
            hash = 37 * hash + this.Type.GetHashCode();
            hash = 37 * hash + this.RawValue.GetHashCode();
            return hash;
        }

        #endregion

        #region GetBytesCount

        internal int? Length = null;

        /// <summary>
        /// Returns how many bytes this BsonValue will use to persist in index writes
        /// </summary>
        public int GetBytesCount(bool recalc)
        {
            if (recalc == false && this.Length.HasValue) return this.Length.Value;

            switch (this.Type)
            {
                case BsonType.Null:
                case BsonType.MinValue:
                case BsonType.MaxValue:
                    this.Length = 0; break;

                case BsonType.Int32: this.Length = 4; break;
                case BsonType.Int64: this.Length = 8; break;
                case BsonType.Double: this.Length = 8; break;
                case BsonType.Decimal: this.Length = 16; break;

                case BsonType.String: this.Length = Encoding.UTF8.GetByteCount((string)this.RawValue); break;

                case BsonType.Binary: this.Length = ((Byte[])this.RawValue).Length; break;
                case BsonType.ObjectId: this.Length = 12; break;
                case BsonType.Guid: this.Length = 16; break;

                case BsonType.Boolean: this.Length = 1; break;
                case BsonType.DateTime: this.Length = 8; break;

                // for Array/Document calculate from elements
                case BsonType.Array:
                    var array = (List<BsonValue>)this.RawValue;
                    this.Length = 5; // header + footer
                    for (var i = 0; i < array.Count; i++)
                    {
                        this.Length += this.GetBytesCountElement(BsonWriter.IntToString(i), array[i] ?? BsonValue.Null, recalc);
                    }
                    break;

                case BsonType.Document:
                    var doc = (Dictionary<string, BsonValue>)this.RawValue;
                    this.Length = 5; // header + footer
                    foreach (var key in doc.Keys)
                    {
                        this.Length += this.GetBytesCountElement(key, doc[key] ?? BsonValue.Null, recalc);
                    }
                    break;
            }

            return this.Length.Value;
        }

        private int GetBytesCountElement(string key, BsonValue value, bool recalc)
        {
            return
                1 + // element type
                Encoding.UTF8.GetByteCount(key) + // CString
                1 + // CString 0x00
                value.GetBytesCount(recalc) +
                (value.Type == BsonType.String || value.Type == BsonType.Binary || value.Type == BsonType.Guid ? 5 : 0); // bytes.Length + 0x??
        }

        #endregion
    }
}
