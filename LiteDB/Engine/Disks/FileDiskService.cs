using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace LiteDB
{
    /// <summary>
    /// Implement NTFS File disk
    /// </summary>
    public class FileDiskService : IDiskService
    {
        /// <summary>
        /// Position, on page, about page type
        /// </summary>
        private const int PAGE_TYPE_POSITION = 4;

        /// <summary>
        /// Map lock positions
        /// </summary>
        internal const int LOCK_INITIAL_POSITION = BasePage.PAGE_SIZE; // use second page
        internal const int LOCK_READ_LENGTH = 1;
        internal const int LOCK_WRITE_LENGTH = 3000;

        private FileStream _stream;
        private string _filename;

        private Logger _log; // will be initialize in "Initialize()"
        private FileOptions _options;

        private Random _lockReadRand = new Random();

        #region Initialize/Dispose disk

        public FileDiskService(string filename, bool journal = true)
            : this(filename, new FileOptions { Journal = journal })
        {
        }

        public FileDiskService(string filename, FileOptions options)
        {
            // simple validations
            if (filename.IsNullOrWhiteSpace()) throw new ArgumentNullException(nameof(filename));
            if (options.InitialSize > options.LimitSize) throw new ArgumentException("limit size less than initial size");

            // setting class variables
            _filename = filename;
            _options = options;
        }

        public void Initialize(Logger log, string password)
        {
            // get log instance to disk
            _log = log;

            // if is read only, journal must be disabled
            if (_options.FileMode == FileMode.ReadOnly) _options.Journal = false;

            _log.Write(Logger.DISK, "open datafile '{0}'", Path.GetFileName(_filename));

            // open/create file using read only/exclusive options
            _stream = this.CreateFileStream(_filename,
                _options.FileMode == FileMode.ReadOnly ? System.IO.FileMode.Open : System.IO.FileMode.OpenOrCreate,
                _options.FileMode == FileMode.ReadOnly ? FileAccess.Read : FileAccess.ReadWrite,
                _options.FileMode == FileMode.Exclusive ? FileShare.None : FileShare.ReadWrite);

            // if file is new, initialize
            if (_stream.Length == 0)
            {
                _log.Write(Logger.DISK, "initialize new datafile");

                // create datafile
                LiteEngine.CreateDatabase(_stream, password, _options.InitialSize);
            }
        }

        public virtual void Dispose()
        {
            if (_stream != null)
            {
                _log.Write(Logger.DISK, "close datafile '{0}'", Path.GetFileName(_filename));
                _stream.Dispose();
                _stream = null;
            }
        }

        #endregion

        #region Read/Write

        /// <summary>
        /// Read page bytes from disk
        /// </summary>
        public virtual byte[] ReadPage(uint pageID)
        {
            var buffer = new byte[BasePage.PAGE_SIZE];
            var position = BasePage.GetSizeOfPages(pageID);

            lock (_stream)
            {
                // position cursor
                if (_stream.Position != position)
                {
                    _stream.Seek(position, SeekOrigin.Begin);
                }

                // read bytes from data file
                _stream.Read(buffer, 0, BasePage.PAGE_SIZE);
            }

            _log.Write(Logger.DISK, "read page #{0:0000} :: {1}", pageID, (PageType)buffer[PAGE_TYPE_POSITION]);

            return buffer;
        }

        /// <summary>
        /// Persist single page bytes to disk
        /// </summary>
        public virtual void WritePage(uint pageID, byte[] buffer, int offset, int pageCount)
        {
            var position = BasePage.GetSizeOfPages(pageID);

            _log.Write(Logger.DISK, "write page #{0:0000} :: {1}", pageID, (PageType)buffer[PAGE_TYPE_POSITION]);

            // position cursor
            if (_stream.Position != position)
            {
                _stream.Seek(position, SeekOrigin.Begin);
            }

            _stream.Write(buffer, offset, BasePage.PAGE_SIZE * pageCount);
        }

        /// <summary>
        /// Set datafile length
        /// </summary>
        public void SetLength(long fileSize)
        {
            // checks if new fileSize will exceed limit size
            if (fileSize > _options.LimitSize) throw LiteException.FileSizeExceeded(_options.LimitSize);

            // fileSize parameter tell me final size of data file - helpful to extend first datafile
            _stream.SetLength(fileSize);
        }

        /// <summary>
        /// Returns file length
        /// </summary>
        public long FileLength { get { return _stream.Length; } }

        #endregion

        #region Journal file

        /// <summary>
        /// Indicate if journal are enabled or not based on file options
        /// </summary>
        public bool IsJournalEnabled { get { return _options.Journal; } }


        /// <summary>
        /// Write original bytes page in a journal file (in sequence) - if journal not exists, create.
        /// </summary>
        public void WriteJournal(byte[] buffer, uint lastPageID, uint count)
        {
            // write journal only if enabled
            if (_options.Journal == false) return;

            _log.Write(Logger.JOURNAL, "extend datafile to journal - {0} pages", count);

            //// set journal file length before write
            //_stream.SetLength((lastPageID + 1 + count) * BasePage.PAGE_SIZE);

            // go to initial file position (after lastPageID)
            _stream.Seek(BasePage.GetSizeOfPages(lastPageID + 1), SeekOrigin.Begin);

            if (count > 0)
                _stream.Write(buffer, 0, (int)count * BasePage.PAGE_SIZE);

            _log.Write(Logger.JOURNAL, "flush journal to disk");

            // ensure all data are persisted in disk
            this.Flush();
        }

        /// <summary>
        /// Read journal file returning IEnumerable of pages
        /// </summary>
        public IEnumerable<byte[]> ReadJournal(uint lastPageID)
        {
            // position stream at begin journal area
            var pos = BasePage.GetSizeOfPages(lastPageID + 1);

            _stream.Seek(pos, SeekOrigin.Begin);

            var buffer = new byte[BasePage.PAGE_SIZE];

            while (_stream.Position < _stream.Length)
            {
                // read page bytes from journal file
                _stream.Read(buffer, 0, BasePage.PAGE_SIZE);

                yield return buffer;

                // now set position to next journal page
                pos += BasePage.PAGE_SIZE;

                _stream.Seek(pos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Shrink datafile to crop journal area
        /// </summary>
        public void ClearJournal(uint lastPageID, bool mustClear)
        {
            // keep some space for future journal to avoid frequently change the file size
            // and boost the journal performance.
            var sizeAllowed = BasePage.GetSizeOfPages(lastPageID + 1 + (uint)(mustClear ? 0 : 64));
            if (this.FileLength > sizeAllowed)
            {
                _log.Write(Logger.JOURNAL, "shrink datafile to remove journal area");
                this.SetLength(sizeAllowed);
            }
            else
            {
                _log.Write(Logger.JOURNAL, "skip shrinking datafile");
            }
        }

        /// <summary>
        /// Flush data from memory to disk
        /// </summary>
        public void Flush()
        {
            _log.Write(Logger.DISK, "flush data from memory to disk");

#if HAVE_FLUSH_DISK
            _stream.Flush(_options.Flush);
#else
            _stream.Flush();
#endif
        }

        #endregion

        #region Lock / Unlock

        /// <summary>
        /// Indicate disk can be access by multiples processes or not
        /// </summary>
        public bool IsExclusive { get { return _options.FileMode == FileMode.Exclusive; } }

        /// <summary>
        /// Implement datafile lock. Return lock position
        /// </summary>
        public int Lock(LockState state, TimeSpan timeout)
        {
#if HAVE_LOCK
            // only shared mode lock datafile
            if (_options.FileMode != FileMode.Shared) return 0;

            var position = state == LockState.Read ? _lockReadRand.Next(LOCK_INITIAL_POSITION, LOCK_INITIAL_POSITION + LOCK_WRITE_LENGTH) : LOCK_INITIAL_POSITION;
            var length = state == LockState.Read ? 1 : LOCK_WRITE_LENGTH;

            _log.Write(Logger.LOCK, "locking file in {0} mode (position: {1})", state, position);

            _stream.TryLock(position, length, timeout);

            return position;
#else
            return 0;
#endif
        }

        /// <summary>
        /// Unlock datafile based on state and position
        /// </summary>
        public void Unlock(LockState state, int position)
        {
#if HAVE_LOCK
            // only shared mode lock datafile
            if (_options.FileMode != FileMode.Shared || state == LockState.Unlocked) return;

            var length = state == LockState.Read ? LOCK_READ_LENGTH : LOCK_WRITE_LENGTH;

            _log.Write(Logger.LOCK, "unlocking file in {0} mode (position: {1})", state, position);

            _stream.TryUnlock(position, length);
#endif
        }

        #endregion

        #region Create Stream

        /// <summary>
        /// Create a new filestream. Can be synced over async task (netstandard)
        /// </summary>
        private FileStream CreateFileStream(string path, System.IO.FileMode mode, FileAccess access, FileShare share)
        {
#if HAVE_SYNC_OVER_ASYNC
            if (_options.Async)
            {
                return System.Threading.Tasks.Task.Run(() => new FileStream(path, mode, access, share, BasePage.PAGE_SIZE))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
            }
#endif
            return new FileStream(path, mode, access, share,
                BasePage.PAGE_SIZE,
                System.IO.FileOptions.RandomAccess);
        }

        #endregion
    }
}