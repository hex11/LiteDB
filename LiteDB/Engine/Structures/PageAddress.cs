using System;
using System.Collections.Generic;

namespace LiteDB
{
    /// <summary>
    /// Represents a page address inside a page structure - index could be byte offset position OR index in a list (6 bytes)
    /// </summary>
    internal struct PageAddress
    {
        public const int SIZE = 6;

        public static PageAddress Empty = new PageAddress(uint.MaxValue, ushort.MaxValue);

        /// <summary>
        /// PageID (4 bytes)
        /// </summary>
        public uint PageID;

        /// <summary>
        /// Index inside page (2 bytes)
        /// </summary>
        public ushort Index;

        public bool IsEmpty
        {
            get { return PageID == uint.MaxValue; }
        }

        public override bool Equals(object obj)
        {
            if (obj is PageAddress other)
                return this == other;
            return false;
        }

        // use this to avoid boxing
        public bool Equals(PageAddress other)
        {
            return this == other;
        }

        public static bool operator ==(PageAddress a, PageAddress b)
        {
            return a.PageID == b.PageID && a.Index == b.Index;
        }

        public static bool operator !=(PageAddress a, PageAddress b)
        {
            return !(a == b);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                // Maybe nullity checks, if these are objects not primitives!
                hash = hash * 23 + (int)this.PageID;
                hash = hash * 23 + this.Index;
                return hash;
            }
        }

        public PageAddress(uint pageID, ushort index)
        {
            PageID = pageID;
            Index = index;
        }

        public override string ToString()
        {
            return IsEmpty ? "----" : PageID.ToString() + ":" + Index.ToString();
        }
    }
}