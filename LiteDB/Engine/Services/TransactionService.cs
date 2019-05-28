using System;
using System.Collections.Generic;
using System.Linq;

namespace LiteDB
{
    /// <summary>
    /// Manages all transactions and grantees concurrency and recovery
    /// </summary>
    internal class TransactionService
    {
        private IDiskService _disk;
        private AesEncryption _crypto;
        private LockService _locker;
        private PageService _pager;
        private CacheService _cache;
        private Logger _log;
        private int _cacheSize;

        internal TransactionService(IDiskService disk, AesEncryption crypto, PageService pager, LockService locker, CacheService cache, int cacheSize, Logger log)
        {
            _disk = disk;
            _crypto = crypto;
            _cache = cache;
            _locker = locker;
            _pager = pager;
            _cacheSize = cacheSize;
            _log = log;
        }

        /// <summary>
        /// Checkpoint is a safe point to clear cache pages without loose pages references.
        /// Is called after each document insert/update/deleted/indexed/fetch from query
        /// Clear only clean pages - do not clear dirty pages (transaction)
        /// Return true if cache was clear
        /// </summary>
        public bool CheckPoint()
        {
            if (_cache.CleanUsed > _cacheSize)
            {
                _log.Write(Logger.CACHE, "cache size reached {0} pages, will clear now", _cache.CleanUsed);

                _cache.ClearPages();

                return true;
            }

            return false;
        }

        const int JOURNAL_BUF_PAGES = 64;
        WeakReference reuseJournalBuffer;
        List<BasePage> reusePageList;

        static readonly Comparison<BasePage> pageSorter = (a, b) =>
        {
            if (a.PageID == 0) return int.MaxValue;
            if (b.PageID == 0) return int.MinValue;
            return a.PageID.CompareTo(b.PageID);
        };

        /// <summary>
        /// Save all dirty pages to disk
        /// </summary>
        public void PersistDirtyPages()
        {
            // get header page
            var header = _pager.GetPage<HeaderPage>(0);

            // increase file changeID (back to 0 when overflow)
            header.ChangeID = header.ChangeID == ushort.MaxValue ? (ushort)0 : (ushort)(header.ChangeID + (ushort)1);

            // mark header as dirty
            _pager.SetDirty(header);

            _log.Write(Logger.DISK, "begin disk operations - changeID: {0}", header.ChangeID);

            var unsortedPages = _cache.GetDirtyPages();

            if (reusePageList == null) reusePageList = new List<BasePage>(unsortedPages.Count);

            reusePageList.AddRange(unsortedPages);

            var dirtyPages = reusePageList;

            byte[] journalBuf = null;

            if (_disk.IsJournalEnabled)
            {
                // sort and ensure the header page is the last
                // the result list will be like: [1, 2, 3, 0]
                dirtyPages.Sort(pageSorter);

                journalBuf = reuseJournalBuffer?.Target as byte[];
                if (journalBuf == null)
                {
                    journalBuf = new byte[JOURNAL_BUF_PAGES * BasePage.PAGE_SIZE];
                    if (reuseJournalBuffer == null) reuseJournalBuffer = new WeakReference(null);
                    reuseJournalBuffer.Target = journalBuf;
                }

                uint bufCur = 0;
                uint pageOffset = header.LastPageID;
                for (int i = 0; i < dirtyPages.Count; i++)
                {
                    var p = dirtyPages[i];
                    _log.Write(Logger.JOURNAL, "write page #{0:0000} :: {1}", p.PageID, p.PageType);

                    // write page bytes to buffer
                    p.WritePage(journalBuf, (int)bufCur * BasePage.PAGE_SIZE);
                    bufCur++;
                    if (bufCur == JOURNAL_BUF_PAGES || i == dirtyPages.Count - 1)
                    {
                        _disk.WriteJournal(journalBuf, pageOffset, bufCur);
                        pageOffset += bufCur;
                        bufCur = 0;
                    }
                }

                // mark header as recovery before start writing (in journal, must keep recovery = false)
                header.Recovery = true;
            }
            else
            {
                // if no journal extend, resize file here to fast writes
                _disk.SetLength(BasePage.GetSizeOfPages(header.LastPageID + 1));
            }

            var reusedBuffer = new byte[BasePage.PAGE_SIZE];
            byte[] encryptBuffer = _crypto == null ? null : new byte[BasePage.PAGE_SIZE];

            // header page (id=0) always must be first page to write on disk because it's will mark disk as "in recovery".
            header.WritePage(reusedBuffer);
            _disk.WritePage(header.PageID, reusedBuffer, 0, 1);

            if (_disk.IsJournalEnabled && dirtyPages.Count <= JOURNAL_BUF_PAGES)
            {
                // then we can just copy from the journal buffer.
                // the dirtyPages is sorted and the header page is always the last.
                for (int i = 0; i < dirtyPages.Count;)
                {
                    var pageId = dirtyPages[i].PageID;
                    if (_crypto != null && pageId != 0)
                    {
                        var buffer = reusedBuffer;
                        Buffer.BlockCopy(journalBuf, i * BasePage.PAGE_SIZE, buffer, 0, BasePage.PAGE_SIZE);
                        buffer = _crypto.Encrypt(buffer, encryptBuffer);
                        _disk.WritePage(pageId, buffer, 0, 1);
                        i++;
                    }
                    else
                    {
                        var beginI = i;
                        i++;
                        if (_crypto == null)
                        {
                            // try to merge mulitple writing operations
                            var lastPageId = pageId;
                            while (i < dirtyPages.Count)
                            {
                                var curI = i;
                                var curPageId = dirtyPages[curI].PageID;
                                if (curPageId == lastPageId + 1)
                                {
                                    lastPageId = curPageId;
                                    i++;
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                        _disk.WritePage(pageId, journalBuf, beginI * BasePage.PAGE_SIZE, i - beginI);
                    }
                }
                header.Recovery = false;
            }
            else
            {
                // write rest pages
                foreach (var page in dirtyPages)
                {
                    if (page.PageID == 0) continue;

                    var buffer = reusedBuffer;
                    page.WritePage(buffer);
                    if (_crypto != null && page.PageID != 0)
                        buffer = _crypto.Encrypt(buffer, encryptBuffer);

                    _disk.WritePage(page.PageID, buffer, 0, 1);
                }

                if (_disk.IsJournalEnabled)
                {
                    // re-write header page but now with recovery=false
                    header.Recovery = false;
                    header.WritePage(reusedBuffer);

                    _log.Write(Logger.DISK, "re-write header page now with recovery = false");

                    _disk.WritePage(header.PageID, reusedBuffer, 0, 1);
                }
            }

            reusePageList.Clear();

            // mark all dirty pages as clean pages (all are persisted in disk and are valid pages)
            _cache.MarkDirtyAsClean();

            // flush all data direct to disk
            _disk.Flush();

            // discard journal file and ensure the journal is cleared when encryption is enabled.
            _disk.ClearJournal(header.LastPageID, _crypto != null);
        }

        /// <summary>
        /// Get journal pages and override all into datafile
        /// </summary>
        public void Recovery()
        {
            _log.Write(Logger.RECOVERY, "initializing recovery mode");

            using (_locker.Write())
            {
                // double check in header need recovery (could be already recover from another thread)
                var header = BasePage.ReadPage(_disk.ReadPage(0)) as HeaderPage;

                if (header.Recovery == false) return;

                byte[] headerBuffer = null;

                byte[] encryptBuffer = _crypto == null ? null : new byte[BasePage.PAGE_SIZE];

                // read all journal pages
                foreach (var buffer_ in _disk.ReadJournal(header.LastPageID))
                {
                    var buffer = buffer_;
                    // read pageID (first 4 bytes)
                    var pageID = BitConverter.ToUInt32(buffer, 0);

                    // don't write the header page now, write it in the end
                    if (pageID == 0)
                    {
                        headerBuffer = buffer;
                        // there may be some unused space after header page.
                        // (assuming header page is the last page in journal)
                        break;
                    }

                    _log.Write(Logger.RECOVERY, "recover page #{0:0000}", pageID);

                    if (_crypto != null) buffer = _crypto.Encrypt(buffer, encryptBuffer);

                    // write in stream (encrypt if datafile is encrypted)
                    _disk.WritePage(pageID, buffer, 0, 1);
                }

                // write header page
                // (header page is always in journal?)
                if (headerBuffer != null)
                {
                    _disk.WritePage(0, headerBuffer, 0, 1);
                }

                // shrink datafile
                _disk.ClearJournal(header.LastPageID, _crypto != null);
            }
        }
    }
}