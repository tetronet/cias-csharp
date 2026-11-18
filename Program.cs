// FCIAS address machine, C# port of ci_address_fcias_purews.js  (targets .NET 9)
//
// Uncomment the next line to enable debug output. When it is NOT defined, every Dbg(...) call
// (including building its interpolated string) is removed at compile time, so it costs nothing.
// #define FCIAS_DEBUG

using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

internal static class Fcias
{
    // === HOW MANY DIGITS WILL BE GENERATED FOR SLAVES? ===
    private const int SmlaLength = 3;

    // === EXTERNAL LINE, ENTER YOUR SERVER (e.g. ws(s)://<domain or ip>/) ===
    private const string ExternalUrl = "wss://data-set.su:3000/";
    private const int ReconnectDelayMs = 3000;
    private const int HeartbeatMs = 25000;

    // === INTERNAL LINE PORT ===
    private const int InternalPort = 30000;

    // === LIMITS ===
    private const int MaxPayload = 10 * 1024 * 1024;   // max size of one WebSocket message
    private const int MaxInternalPackageData = 87385;  // package_data length limit on the internal line
    private const long OutboxMaxBytes = 4 * 1024 * 1024; // queued bytes per connection; when full, new frames are dropped
                                                         // (a single frame is always accepted into an empty queue)
    private const int FrameOverhead = 64;               // counted per queued frame, so tiny frames can't pile up endlessly
    private const int MaxAddressChars = 512;            // longer "to" addresses go through the slow (DOM) path

    // === LATENCY UNDER LOAD ===
    // Frames up to this size (pings, commands, acks) jump ahead of bulk frames in a peer's outbox.
    private const int UrgentFrameBytes = 4 * 1024;
    // Backpressure: when a slave's outbox holds more than this, we stop reading the INTERNAL sender
    // that feeds it until it drains to half. The sender's TCP window then slows it down (NetStream
    // just sends slower) instead of the router buffering megabytes and burning CPU/RAM on it.
    private const long BackpressureBytes = 512 * 1024;
    private static readonly TimeSpan BackpressureMaxWait = TimeSpan.FromSeconds(2); // stuck receiver -> old drop behaviour
    // A busy receive/write loop completes synchronously while data is buffered and never gives its
    // thread back. On 1-2 cores two such loops (bulk sender + bulk receiver) occupy the whole thread
    // pool and every other connection (e.g. pings) waits until the pool injects threads (~0.5 s steps).
    // So each loop yields after this many bytes.
    private const int YieldEveryBytes = 128 * 1024;

    // === PERFORMANCE SWITCHES ===
    // Run receive handlers right on the socket I/O thread (no thread-pool hop per packet).
    // Safe because the handlers never block. For full effect also set the environment variable
    // DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1 (Linux).
    private const bool InlineSocketScheduling = true;
    // Merge several queued frames on the external line into fewer TCP writes (fewer syscalls at high packet rate).
    private const bool CorkExternalWrites = true;
    private const int CorkBufferSize = 64 * 1024;

    private const int RecvBufDefault = 64 * 1024;      // receive buffer per connection
    private const int RecvBufShrinkAbove = 256 * 1024; // after a bigger message the buffer goes back to the default size

    // === LOCAL ADDRESS, CAN BE CHANGED AT RUNTIME BY THE REMOTE HOST ===
    private static readonly string AddressFile =
        Path.Combine(AppContext.BaseDirectory, "ci_address_fcias_purews.txt");
    private static volatile string LocalAddress = "";

    // === RUNTIME ONLY ===
    private static readonly ConcurrentDictionary<string, Peer> SocketMap = new(StringComparer.Ordinal); // address -> peer
    private static readonly ConcurrentDictionary<string, byte> BannedAddresses = new(StringComparer.Ordinal);
    // span-keyed views of the same dictionaries: lookups without allocating a string
    private static readonly ConcurrentDictionary<string, Peer>.AlternateLookup<ReadOnlySpan<char>> SocketLookup =
        SocketMap.GetAlternateLookup<ReadOnlySpan<char>>();
    private static readonly ConcurrentDictionary<string, byte>.AlternateLookup<ReadOnlySpan<char>> BannedLookup =
        BannedAddresses.GetAlternateLookup<ReadOnlySpan<char>>();
    private static volatile Peer? _external;

    // UnsafeRelaxedJsonEscaping: do NOT turn '+' (base64!) and non-ASCII into \uXXXX escapes
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    [Conditional("FCIAS_DEBUG")]
    private static void Dbg(string text) => Console.WriteLine(text);

    // =====================================================================
    // === ENTRY POINT                                                   ===
    // =====================================================================
    private static async Task Main()
    {
        LocalAddress = File.ReadAllText(AddressFile).Trim();

        _ = Task.Run(ExternalLoopAsync);

        while (LocalAddress == "")
        {
            await Task.Delay(100);
        }

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.AddServerHeader = false;
            o.ListenAnyIP(InternalPort);
            // let several frames be in flight to a slave before SendAsync has to wait for the socket
            o.Limits.MaxResponseBufferSize = 256 * 1024;
        });
        builder.WebHost.UseSockets(o => o.UnsafePreferInlineScheduling = InlineSocketScheduling);

        var app = builder.Build();
        app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromMilliseconds(HeartbeatMs) });
        app.Run(async ctx =>
        {
            if (!ctx.WebSockets.IsWebSocketRequest)
            {
                ctx.Response.StatusCode = 400;
                return;
            }

            WebSocket ws;
            try
            {
                ws = await ctx.WebSockets.AcceptWebSocketAsync();
            }
            catch (Exception e)
            {
                Dbg("WebSocket accept failed: " + e.Message);
                return;
            }
            await HandleInternalClientAsync(ws);
        });

        await app.StartAsync();
        Console.WriteLine($"FCIAS: internal line listening on port {InternalPort}, local address \"{LocalAddress}\"");
        await app.WaitForShutdownAsync();
    }

    // =====================================================================
    // === FRAMES: pooled byte buffers                                    ===
    // =====================================================================
    // Every outgoing frame lives in a buffer rented from ArrayPool. The writer loop returns it
    // after SendAsync (or Send returns it right away if the frame is dropped).
    private readonly struct Frame(byte[] buf, int len)
    {
        public readonly byte[] Buf = buf;
        public readonly int Len = len;
    }

    private static Frame RentFrame(int len) => new(ArrayPool<byte>.Shared.Rent(len), len);

    private static Frame CopyFrame(ReadOnlySpan<byte> src)
    {
        var f = RentFrame(src.Length);
        src.CopyTo(f.Buf);
        return f;
    }

    private static Frame StringFrame(string json)
    {
        var f = RentFrame(Encoding.UTF8.GetByteCount(json));
        Encoding.UTF8.GetBytes(json, f.Buf);
        return f;
    }

    private static void Release(Frame f) => ArrayPool<byte>.Shared.Return(f.Buf);

    // =====================================================================
    // === PEER: one WebSocket + one outgoing queue + one writer task      ===
    // =====================================================================
    // A WebSocket allows only ONE SendAsync at a time, and many threads may want to
    // send to the same slave. So every send goes through this queue.
    private sealed class Peer
    {
        public readonly WebSocket Socket;
        public volatile string? Address;
        public volatile string? DeviceType;
        private readonly CorkStream? _cork;
        private readonly ConcurrentQueue<Frame> _urgent = new(); // small frames: always sent before bulk ones
        private readonly ConcurrentQueue<Frame> _outbox = new(); // bulk frames
        // "doorbell": rung after every enqueue, the writer loop sleeps on it when both queues are empty
        private readonly Channel<bool> _bell = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false
        });
        private volatile bool _completed;
        private long _queuedBytes;
        private readonly object _spaceLock = new();
        private TaskCompletionSource? _spaceWaiter;

        public Peer(WebSocket socket, CorkStream? cork = null)
        {
            Socket = socket;
            _cork = cork;
        }

        // Takes ownership of the frame's buffer.
        public void Send(Frame frame)
        {
            long cost = frame.Len + FrameOverhead;
            long total = Interlocked.Add(ref _queuedBytes, cost);
            // a stalled client can't eat all RAM: drop when over budget (but always accept into an empty queue)
            if (_completed || (total > OutboxMaxBytes && total != cost))
            {
                Interlocked.Add(ref _queuedBytes, -cost);
                Release(frame);
                return;
            }
            (frame.Len <= UrgentFrameBytes ? _urgent : _outbox).Enqueue(frame);
            _bell.Writer.TryWrite(true);
        }

        public void Complete()
        {
            _completed = true;
            _bell.Writer.TryComplete();
        }

        // Completes when the outbox is back under the backpressure limit (or after BackpressureMaxWait).
        public async ValueTask WaitForSpaceAsync()
        {
            Task wait;
            lock (_spaceLock)
            {
                if (Interlocked.Read(ref _queuedBytes) <= BackpressureBytes) return;
                _spaceWaiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                wait = _spaceWaiter.Task;
            }
            try { await wait.WaitAsync(BackpressureMaxWait); }
            catch (TimeoutException) { }
        }

        private void SignalSpace(bool force = false)
        {
            if (Volatile.Read(ref _spaceWaiter) is null) return;
            lock (_spaceLock)
            {
                if (_spaceWaiter is null) return;
                if (!force && Interlocked.Read(ref _queuedBytes) > BackpressureBytes / 2) return;
                _spaceWaiter.TrySetResult();
                _spaceWaiter = null;
            }
        }

        private bool TryDequeue(out Frame frame) =>
            _urgent.TryDequeue(out frame) || _outbox.TryDequeue(out frame);

        private bool HasMore() => !_urgent.IsEmpty || !_outbox.IsEmpty;

        // false when the peer is completed and both queues are empty
        private async ValueTask<bool> WaitForFramesAsync()
        {
            while (!HasMore())
            {
                if (!await _bell.Reader.WaitToReadAsync()) return HasMore();
                _bell.Reader.TryRead(out _);
            }
            return true;
        }

        public async Task WriteLoopAsync()
        {
            try
            {
                while (await WaitForFramesAsync())
                {
                    bool corked = false;
                    int sinceYield = 0;
                    try
                    {
                        while (TryDequeue(out var frame))
                        {
                            Interlocked.Add(ref _queuedBytes, -(frame.Len + FrameOverhead));
                            SignalSpace();
                            // more frames waiting -> collect their bytes and write them out together
                            if (_cork is not null && !corked && HasMore())
                            {
                                _cork.Cork();
                                corked = true;
                            }
                            try
                            {
                                await Socket.SendAsync(frame.Buf.AsMemory(0, frame.Len), WebSocketMessageType.Text, true, CancellationToken.None);
                            }
                            finally
                            {
                                Release(frame);
                            }
                            sinceYield += frame.Len;
                            if (sinceYield >= YieldEveryBytes)
                            {
                                sinceYield = 0;
                                if (corked) { await _cork!.UncorkAsync(); corked = false; }
                                await Task.Yield(); // let other connections run
                            }
                        }
                    }
                    finally
                    {
                        if (corked) await _cork!.UncorkAsync();
                    }
                }
            }
            catch (Exception e)
            {
                Dbg("Write loop ended: " + e.Message);
            }
            finally
            {
                Complete(); // socket is dead, stop accepting frames
                while (TryDequeue(out var frame)) Release(frame);
                Interlocked.Exchange(ref _queuedBytes, 0);
                SignalSpace(force: true); // don't leave a sender waiting on a dead peer
            }
        }
    }

    // =====================================================================
    // === CORK STREAM: write batching under the external WebSocket/TLS    ===
    // =====================================================================
    // Sits between the TCP socket and SslStream. Normally writes pass straight through.
    // While "corked" (the writer loop has several frames queued) writes are collected in a buffer
    // and sent in one go on Uncork or when the buffer fills up.
    private sealed class CorkStream(Stream inner) : Stream
    {
        private readonly SemaphoreSlim _lock = new(1, 1);
        private readonly byte[] _buf = new byte[CorkBufferSize];
        private int _len;
        private volatile bool _corked;

        public void Cork() => _corked = true;

        public async ValueTask UncorkAsync()
        {
            await _lock.WaitAsync();
            try
            {
                _corked = false;
                await FlushBufferAsync(default);
            }
            finally
            {
                _lock.Release();
            }
        }

        private async ValueTask FlushBufferAsync(CancellationToken ct)
        {
            if (_len == 0) return;
            int n = _len;
            _len = 0;
            await inner.WriteAsync(_buf.AsMemory(0, n), ct);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            await _lock.WaitAsync(ct);
            try
            {
                if (_corked && data.Length <= _buf.Length - _len)
                {
                    data.Span.CopyTo(_buf.AsSpan(_len));
                    _len += data.Length;
                    return;
                }
                await FlushBufferAsync(ct);
                if (_corked && data.Length < _buf.Length)
                {
                    data.Span.CopyTo(_buf);
                    _len = data.Length;
                    return;
                }
                await inner.WriteAsync(data, ct);
            }
            finally
            {
                _lock.Release();
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            WriteAsync(buffer.AsMemory(offset, count), ct).AsTask();

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> data)
        {
            _lock.Wait();
            try
            {
                if (_len > 0)
                {
                    inner.Write(_buf, 0, _len);
                    _len = 0;
                }
                inner.Write(data);
            }
            finally
            {
                _lock.Release();
            }
        }

        public override async Task FlushAsync(CancellationToken ct)
        {
            await _lock.WaitAsync(ct);
            try
            {
                await FlushBufferAsync(ct);
                await inner.FlushAsync(ct);
            }
            finally
            {
                _lock.Release();
            }
        }

        public override void Flush() => FlushAsync(default).GetAwaiter().GetResult();

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) => inner.ReadAsync(buffer, ct);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) => inner.ReadAsync(buffer, offset, count, ct);
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => inner.Read(buffer);

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    // Reusable per-connection receive buffer, rented from ArrayPool (grows up to MaxPayload for big messages)
    private sealed class RecvBuf
    {
        public byte[] Data = ArrayPool<byte>.Shared.Rent(RecvBufDefault);

        public void Grow()
        {
            var bigger = ArrayPool<byte>.Shared.Rent(Math.Min(Data.Length * 2, MaxPayload));
            Data.CopyTo(bigger, 0);
            ArrayPool<byte>.Shared.Return(Data);
            Data = bigger;
        }

        // one big message shouldn't pin megabytes to the connection forever
        public void Shrink()
        {
            if (Data.Length <= RecvBufShrinkAbove) return;
            ArrayPool<byte>.Shared.Return(Data);
            Data = ArrayPool<byte>.Shared.Rent(RecvBufDefault);
        }

        public void Free()
        {
            ArrayPool<byte>.Shared.Return(Data);
            Data = [];
        }
    }

    // Reads one complete message (handles fragmentation). Returns length, or -1 on close / too big.
    private static async ValueTask<int> ReceiveMessageAsync(WebSocket ws, RecvBuf rb)
    {
        int total = 0;
        while (true)
        {
            int cap = Math.Min(rb.Data.Length, MaxPayload);
            if (total == cap)
            {
                if (cap >= MaxPayload)
                {
                    try { await ws.CloseOutputAsync(WebSocketCloseStatus.MessageTooBig, "too big", CancellationToken.None); }
                    catch { }
                    return -1;
                }
                rb.Grow();
                continue;
            }

            var r = await ws.ReceiveAsync(rb.Data.AsMemory(total, cap - total), CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) return -1;

            total += r.Count;
            if (r.EndOfMessage) return total;
        }
    }

    // =====================================================================
    // === WIRE FORMAT: JSON array ["event", data?]  (both lines)          ===
    // =====================================================================
    private static Frame BuildFrame(string ev, JsonNode? data = null)
    {
        var arr = data is null ? new JsonArray(JsonValue.Create(ev)) : new JsonArray(JsonValue.Create(ev), data);
        return StringFrame(arr.ToJsonString(JsonOpts));
    }

    // first byte that isn't JSON whitespace, or -1
    private static int FirstNonWhitespace(ReadOnlySpan<byte> s)
    {
        foreach (byte b in s)
            if (b is not ((byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n')) return b;
        return -1;
    }

    // Returns the parsed frame, or null if the message isn't a valid ["event", ...] array.
    private static JsonArray? ParseFrame(byte[] buf, int len, out string ev)
    {
        ev = "";
        // cheap pre-check: anything that doesn't start with '[' (or a UTF-8 BOM) can't be an array,
        // so don't pay for a JsonException on raw addresses like 1-2-003
        int first = FirstNonWhitespace(buf.AsSpan(0, len));
        if (first != '[' && first != 0xEF) return null;
        try
        {
            if (JsonNode.Parse(buf.AsSpan(0, len)) is JsonArray arr
                && arr.Count > 0
                && arr[0] is JsonValue v
                && v.TryGetValue<string>(out var name))
            {
                ev = name;
                return arr;
            }
        }
        catch (JsonException) { }
        return null;
    }

    // =====================================================================
    // === FAST PATH: "data-transmission" without building a JSON tree     ===
    // =====================================================================
    // One forward pass of Utf8JsonReader over the raw bytes: picks up the "to" blocks, the position
    // of max_hops and the package_data length. Nothing is allocated, package_data is never copied
    // into a string. Anything unusual (escaped/duplicate keys, odd "to" elements, invalid JSON...)
    // returns false and the frame goes through the normal DOM path, which keeps the old behaviour.
    private const int MaxScannedProps = 16;

    private static bool TryScanPacket(ReadOnlySpan<byte> json, Span<char> to, bool needDataLength,
        out int toLen, out int hopsStart, out int hopsLen, out double hops, out int dataChars)
    {
        toLen = 0; hopsStart = -1; hopsLen = 0; hops = 0; dataChars = 0;
        if (FirstNonWhitespace(json) != '[') return false;
        try
        {
            var r = new Utf8JsonReader(json);
            if (!r.Read() || r.TokenType != JsonTokenType.StartArray) return false;
            if (!r.Read() || r.TokenType != JsonTokenType.String || !r.ValueTextEquals("data-transmission"u8)) return false;
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject) return false;

            bool haveInfo = false, haveTo = false, haveData = false;
            Span<int> names = stackalloc int[MaxScannedProps * 2];
            int nameCount = 0;
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                if (!NoteName(ref r, json, names, ref nameCount)) return false;
                if (r.ValueTextEquals("package_info"u8))
                {
                    r.Read();
                    if (r.TokenType != JsonTokenType.StartObject) return false;
                    if (!ScanInfo(ref r, json, to, ref toLen, ref haveTo, ref hopsStart, ref hopsLen, ref hops)) return false;
                    haveInfo = true;
                }
                else if (r.ValueTextEquals("package_data"u8))
                {
                    r.Read();
                    if (r.TokenType != JsonTokenType.String) return false;
                    if (r.ValueIsEscaped) dataChars = UnescapedLength(ref r);
                    else if (needDataLength) dataChars = Encoding.UTF8.GetCharCount(r.ValueSpan);
                    haveData = true;
                }
                else
                {
                    r.Read();
                    r.Skip();
                }
            }

            // rest of the top-level array, then the end of the document (throws on trailing garbage)
            while (r.Read()) r.Skip();
            return haveInfo && haveTo && haveData;
        }
        catch (JsonException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false; // string with an invalid escape (e.g. a lone surrogate): slow path decides
        }
    }

    private static bool ScanInfo(ref Utf8JsonReader r, ReadOnlySpan<byte> json, Span<char> to, ref int toLen, ref bool haveTo,
        ref int hopsStart, ref int hopsLen, ref double hops)
    {
        Span<int> names = stackalloc int[MaxScannedProps * 2];
        int nameCount = 0;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            if (!NoteName(ref r, json, names, ref nameCount)) return false;
            if (r.ValueTextEquals("to"u8))
            {
                r.Read();
                if (r.TokenType != JsonTokenType.StartArray) return false;
                // same text as JoinBlocks(): strings unescaped, numbers as written, joined with '-'
                bool first = true;
                while (r.Read() && r.TokenType != JsonTokenType.EndArray)
                {
                    if (!first)
                    {
                        if (toLen >= to.Length) return false;
                        to[toLen++] = '-';
                    }
                    first = false;

                    if (r.ValueSpan.Length > to.Length - toLen) return false;
                    if (r.TokenType == JsonTokenType.String)
                    {
                        toLen += r.CopyString(to[toLen..]);
                    }
                    else if (r.TokenType == JsonTokenType.Number)
                    {
                        foreach (byte b in r.ValueSpan) to[toLen++] = (char)b;
                    }
                    else
                    {
                        return false; // null / bool / nested: rare, let the DOM path deal with it
                    }
                }
                haveTo = true;
            }
            else if (r.ValueTextEquals("max_hops"u8))
            {
                r.Read();
                // same rule as ConsumeHop(): only a finite number counts as a TTL
                if (r.TokenType == JsonTokenType.Number && r.TryGetDouble(out var h))
                {
                    hopsStart = (int)r.TokenStartIndex;
                    hopsLen = r.ValueSpan.Length;
                    hops = h;
                }
                else
                {
                    r.Skip();
                }
            }
            else
            {
                r.Read();
                r.Skip();
            }
        }
        return true;
    }

    // Remembers property names of one object. Returns false (-> slow path) on an escaped name,
    // a duplicate name or too many properties: those are exactly the cases where the DOM may behave differently.
    private static bool NoteName(ref Utf8JsonReader r, ReadOnlySpan<byte> json, scoped Span<int> names, ref int count)
    {
        if (r.ValueIsEscaped || count == MaxScannedProps) return false;
        var name = r.ValueSpan;
        for (int i = 0; i < count; i++)
            if (json.Slice(names[2 * i], names[2 * i + 1]).SequenceEqual(name)) return false;
        names[2 * count] = (int)r.TokenStartIndex + 1; // skip the opening quote
        names[2 * count + 1] = name.Length;
        count++;
        return true;
    }

    private static int UnescapedLength(ref Utf8JsonReader r)
    {
        char[] tmp = ArrayPool<char>.Shared.Rent(r.ValueSpan.Length);
        try { return r.CopyString(tmp); }
        finally { ArrayPool<char>.Shared.Return(tmp); }
    }

    // Copy of the frame with the max_hops number replaced by (hops - 1). Everything else stays byte-for-byte.
    private static Frame SpliceHops(ReadOnlySpan<byte> src, int start, int oldLen, double hops)
    {
        Span<byte> num = stackalloc byte[24];
        ((long)(hops - 1)).TryFormat(num, out int n, default, CultureInfo.InvariantCulture);
        var f = RentFrame(src.Length - oldLen + n);
        src[..start].CopyTo(f.Buf);
        num[..n].CopyTo(f.Buf.AsSpan(start));
        src[(start + oldLen)..].CopyTo(f.Buf.AsSpan(start + n));
        return f;
    }

    // Returns false if the frame needs the slow path. target = the peer the frame was queued to (if any).
    private static bool TryFastDataTransmission(byte[] buf, int len, bool fromInternal, out Peer? target)
    {
        target = null;
        Span<char> toBuf = stackalloc char[MaxAddressChars];
        var json = buf.AsSpan(0, len);
        if (!TryScanPacket(json, toBuf, fromInternal, out int toLen, out int hopsStart, out int hopsLen, out double hops, out int dataChars))
            return false;
        ReadOnlySpan<char> rawTo = toBuf[..toLen];

        if (hopsStart >= 0 && hops <= 0) return true; // TTL expired
        if ((fromInternal && dataChars >= MaxInternalPackageData) || BannedLookup.ContainsKey(rawTo))
        {
            Dbg($"Forwarding data refused for {rawTo}");
            return true;
        }

        if (TryRouteLocal(rawTo, out var peer)) target = peer;
        else target = fromInternal ? _external : null;

        target?.Send(hopsStart >= 0 ? SpliceHops(json, hopsStart, hopsLen, hops) : CopyFrame(json));
        return true;
    }

    // =====================================================================
    // === HELPERS                                                        ===
    // =====================================================================
    private static string NodeToString(JsonNode? n) =>
        n is JsonValue v && v.TryGetValue<string>(out var s) ? s : n?.ToJsonString() ?? "";

    private static string JoinBlocks(JsonArray a)
    {
        if (a.Count == 1) return NodeToString(a[0]);
        var sb = new StringBuilder();
        for (int i = 0; i < a.Count; i++)
        {
            if (i > 0) sb.Append('-');
            sb.Append(NodeToString(a[i]));
        }
        return sb.ToString();
    }

    // same as JS .replace("*", "") : removes only the FIRST '*'
    private static string WithoutFirstStar(string s)
    {
        int i = s.IndexOf('*');
        return i < 0 ? s : s.Remove(i, 1);
    }

    private static bool IsInOwnNetwork(string[] address)
    {
        string local = LocalAddress;
        int localBlocks = local.Split('-').Length;
        string joined = WithoutFirstStar(string.Join('-', address));

        if (!joined.StartsWith(local, StringComparison.Ordinal)) return false;
        if (joined == local) return true;
        return (address.Length == localBlocks || address.Length - 1 == localBlocks)
               && localBlocks < address.Length
               && address[localBlocks].Length == SmlaLength;
    }

    // TTL handling: no max_hops -> pass through. Returns false if the packet died.
    private static bool ConsumeHop(JsonObject info, ref bool modified)
    {
        if (!info.TryGetPropertyValue("max_hops", out var node) || node is null) return true;
        if (node is not JsonValue v || !v.TryGetValue<double>(out var hops)) return true;
        if (hops <= 0) return false;
        info["max_hops"] = JsonValue.Create((long)(hops - 1));
        modified = true;
        return true;
    }

    private static bool TryReadPacket(JsonArray frame, out JsonObject info, out JsonArray to, out string data)
    {
        info = null!; to = null!; data = null!;
        if (frame.Count < 2 || frame[1] is not JsonObject pkt) return false;
        if (pkt["package_info"] is not JsonObject i) return false;
        if (i["to"] is not JsonArray t) return false;
        if (pkt["package_data"] is not JsonValue dv || !dv.TryGetValue<string>(out var d)) return false;
        info = i; to = t; data = d;
        return true;
    }

    // If the receiver lives in our network: returns true (peer may be null if that slave isn't connected).
    private static bool TryRouteLocal(ReadOnlySpan<char> rawTo, out Peer? peer)
    {
        peer = null;
        string local = LocalAddress;

        // same as WithoutFirstStar(), without allocating
        Span<char> tmp = stackalloc char[MaxAddressChars];
        scoped ReadOnlySpan<char> joined = rawTo;
        int star = rawTo.IndexOf('*');
        if (star >= 0)
        {
            if (rawTo.Length - 1 > tmp.Length)
            {
                joined = WithoutFirstStar(rawTo.ToString());
            }
            else
            {
                rawTo[..star].CopyTo(tmp);
                rawTo[(star + 1)..].CopyTo(tmp[star..]);
                joined = tmp[..(rawTo.Length - 1)];
            }
        }
        if (!joined.StartsWith(local, StringComparison.Ordinal)) return false;

        int keyLen = Math.Min(joined.Length, local.Length + 1 + SmlaLength);
        SocketLookup.TryGetValue(joined[..keyLen], out peer);
        return true;
    }

    // Untouched packets are forwarded byte-for-byte; only packets with a TTL are re-serialized.
    private static Frame FrameBytes(JsonArray frame, bool modified, byte[] buf, int len) =>
        modified ? StringFrame(frame.ToJsonString(JsonOpts)) : CopyFrame(buf.AsSpan(0, len));

    private static void Unbind(Peer peer)
    {
        var addr = peer.Address;
        if (addr is null) return;
        SocketMap.TryRemove(new KeyValuePair<string, Peer>(addr, peer)); // only if it still points at THIS peer
        peer.Address = null;
    }

    // =====================================================================
    // === EXTERNAL LINE (ClientWebSocket, same protocol as internal)      ===
    // =====================================================================
    private static async Task ExternalLoopAsync()
    {
        while (true)
        {
            // our own TCP connect, so a CorkStream can sit under TLS and batch writes
            CorkStream? cork = null;
            using var handler = new SocketsHttpHandler
            {
                ConnectCallback = async (ctx, ct) =>
                {
                    var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    LimitUnsentBytes(s);
                    try
                    {
                        await s.ConnectAsync(ctx.DnsEndPoint, ct);
                    }
                    catch
                    {
                        s.Dispose();
                        throw;
                    }
                    return cork = new CorkStream(new NetworkStream(s, ownsSocket: true));
                }
            };
            using var invoker = new HttpMessageInvoker(handler);
            using var ws = new ClientWebSocket();
            ws.Options.KeepAliveInterval = TimeSpan.FromMilliseconds(HeartbeatMs);
            ws.Options.KeepAliveTimeout = TimeSpan.FromMilliseconds(HeartbeatMs); // dead line -> reconnect (.NET 8+)

            Peer? peer = null;
            Task writer = Task.CompletedTask;
            var rb = new RecvBuf();
            try
            {
                await ws.ConnectAsync(new Uri(ExternalUrl), invoker, CancellationToken.None);
                Dbg("External line connected");

                peer = new Peer(ws, CorkExternalWrites ? cork : null);
                writer = peer.WriteLoopAsync();
                _external = peer;

                int sinceYield = 0;
                while (ws.State == WebSocketState.Open)
                {
                    int n = await ReceiveMessageAsync(ws, rb);
                    if (n < 0) break;
                    try { HandleExternalFrame(rb.Data, n); }
                    catch (Exception e) { Dbg("External frame error: " + e.Message); }
                    rb.Shrink();
                    // no backpressure here: pausing the external line would stall everyone behind it
                    if ((sinceYield += n) >= YieldEveryBytes) { sinceYield = 0; await Task.Yield(); }
                }
            }
            catch (Exception e)
            {
                Dbg("External line error: " + e.Message);
            }
            finally
            {
                _external = null;
                peer?.Complete();
                await writer;
                rb.Free();
            }

            Dbg($"External line closed, reconnecting in {ReconnectDelayMs} ms");
            await Task.Delay(ReconnectDelayMs);
        }
    }

    // Linux: keep at most ExternalNotSentLowat bytes of not-yet-sent data in the kernel socket queue.
    // Without it the kernel buffers megabytes of bulk (NetStream) and every small frame written after
    // it (pings) waits behind all of that, no matter how the router orders its own queue. With it the
    // backlog stays in our outbox, where urgent frames can jump ahead. Throughput isn't limited:
    // bytes already in flight (congestion window) don't count. Not available on Windows.
    // DISABLED: on the Linux server the external line stopped sending with it (async writer never
    // woke up again). Turn on only after testing on the target kernel.
    private const bool UseNotSentLowat = false;
    private const int ExternalNotSentLowat = 128 * 1024;

    private static void LimitUnsentBytes(Socket s)
    {
        if (!UseNotSentLowat || !OperatingSystem.IsLinux()) return;
        try
        {
            const int IPPROTO_TCP = 6, TCP_NOTSENT_LOWAT = 25;
            s.SetRawSocketOption(IPPROTO_TCP, TCP_NOTSENT_LOWAT, BitConverter.GetBytes(ExternalNotSentLowat));
        }
        catch (Exception e)
        {
            Dbg("TCP_NOTSENT_LOWAT not set: " + e.Message);
        }
    }

    private static void SendExternal(string ev, JsonNode? data = null)
    {
        var ext = _external;
        if (ext is not null) ext.Send(BuildFrame(ev, data));
    }

    private static void HandleExternalFrame(byte[] buf, int len)
    {
        if (TryFastDataTransmission(buf, len, fromInternal: false, out _)) return;

        var frame = ParseFrame(buf, len, out var ev);
        if (frame is null) { Dbg("Received malformed external message"); return; }
        JsonNode? data = frame.Count > 1 ? frame[1] : null;

        switch (ev)
        {
            case "get-addr":
                Dbg("REMOTE host is asking for LOCAL address value");
                SendExternal("self-addr", JsonValue.Create(LocalAddress));
                break;

            case "set-modem-local-address":
                if (data is JsonArray blocks)
                {
                    LocalAddress = JoinBlocks(blocks);
                    Dbg("Obtained LOCAL address from REMOTE host: " + LocalAddress);
                    try { File.WriteAllText(AddressFile, LocalAddress, Encoding.UTF8); }
                    catch (Exception e) { Dbg("Could not save address file: " + e.Message); }
                }
                break;

            case "get-device-type":
                Dbg("REMOTE requested LOCAL device type");
                SendExternal("device-type", JsonValue.Create("address-machine"));
                break;

            case "data-transmission":
                {
                    if (!TryReadPacket(frame, out var info, out var to, out _)) return;
                    bool modified = false;
                    if (!ConsumeHop(info, ref modified)) return;

                    string rawTo = JoinBlocks(to);
                    if (BannedAddresses.ContainsKey(rawTo))
                    {
                        Dbg($"Forwarding data refused: address {rawTo} is banned");
                        return;
                    }
                    if (TryRouteLocal(rawTo, out var peer))
                        peer?.Send(FrameBytes(frame, modified, buf, len));
                    break;
                }

            default:
                Dbg("Received unknown external event: " + ev);
                break;
        }
    }

    // =====================================================================
    // === INTERNAL LINE (Kestrel WebSocket server)                        ===
    // =====================================================================
    private static async Task HandleInternalClientAsync(WebSocket ws)
    {
        var peer = new Peer(ws);
        var writer = peer.WriteLoopAsync();
        peer.Send(BuildFrame("get-addr"));

        var rb = new RecvBuf();
        try
        {
            int sinceYield = 0;
            while (ws.State == WebSocketState.Open)
            {
                int n = await ReceiveMessageAsync(ws, rb);
                if (n < 0) break;
                Peer? target = null;
                try { target = HandleInternalFrame(peer, rb.Data, n); }
                catch (Exception e) { Dbg("Internal frame error: " + e.Message); }
                rb.Shrink();
                // receiver can't keep up -> stop reading this sender for a while (TCP slows it down)
                if (target is not null && target != peer) await target.WaitForSpaceAsync();
                if ((sinceYield += n) >= YieldEveryBytes) { sinceYield = 0; await Task.Yield(); }
            }
        }
        catch (WebSocketException) { }
        catch (IOException) { }
        catch (OperationCanceledException) { } // connection aborted / server shutting down
        finally
        {
            Dbg($"Disconnected address: {peer.Address}");
            Unbind(peer);
            peer.Complete();
            await writer; // flush what's queued before closing
            rb.Free();
            try
            {
                if (ws.State is WebSocketState.CloseReceived or WebSocketState.Open)
                    await ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
            }
            catch { }
            ws.Dispose();
        }
    }

    // Returns the peer a data packet was queued to (for backpressure), or null.
    private static Peer? HandleInternalFrame(Peer peer, byte[] buf, int len)
    {
        if (TryFastDataTransmission(buf, len, fromInternal: true, out var fastTarget)) return fastTarget;

        var frame = ParseFrame(buf, len, out var ev);

        // A non-array payload (e.g. a bare "0" or "1-2-003") is an implicit "self-addr",
        // matching the C# client's get-addr reply (raw ASCII address, no JSON wrapper).
        if (frame is null)
        {
            OnSelfAddr(peer, Encoding.UTF8.GetString(buf, 0, len));
            return null;
        }

        JsonNode? data = frame.Count > 1 ? frame[1] : null;
        switch (ev)
        {
            case "device-type":
                OnDeviceType(peer, data);
                break;

            case "self-addr":
                if (data is JsonValue v && v.TryGetValue<string>(out var addr)) OnSelfAddr(peer, addr);
                break;

            case "data-transmission":
                return OnInternalData(frame, buf, len);

            default:
                Dbg("Received unknown event: " + ev);
                break;
        }
        return null;
    }

    private static void OnDeviceType(Peer peer, JsonNode? data)
    {
        string? dtype = data is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;
        if (dtype is "address-machine" or "modem")
        {
            peer.DeviceType = dtype;
            Dbg($"A new {dtype} has recently connected!");
        }
        else
        {
            peer.Send(BuildFrame("?invalid-device-type"));
        }
    }

    private static void OnSelfAddr(Peer peer, string addr)
    {
        if (IsInOwnNetwork(addr.Split('-')))
        {
            Unbind(peer);
            SocketMap[addr] = peer;
            peer.Address = addr;
            peer.Send(BuildFrame("addr-ok"));
        }
        else if (addr is "0" or "")
        {
            // hand out a fresh, unused SMLA address
            Unbind(peer);
            int space = (int)Math.Pow(10, SmlaLength);
            string local = LocalAddress;
            for (int attempt = 0; attempt < 100_000; attempt++)
            {
                string smla = Random.Shared.Next(space).ToString().PadLeft(SmlaLength, '0');
                string joined = local + "-" + smla;
                if (!SocketMap.TryAdd(joined, peer)) continue; // atomic: two slaves can't get the same address

                peer.Address = joined;
                Dbg("SMLAed address for the device: " + joined);

                var blocks = new JsonArray();
                foreach (var b in joined.Split('-')) blocks.Add(b);
                peer.Send(BuildFrame("set-modem-local-address", blocks));
                break;
            }
        }
        else
        {
            peer.Send(BuildFrame("?unknown-addr"));
        }

        peer.Send(BuildFrame("get-device-type"));
    }

    private static Peer? OnInternalData(JsonArray frame, byte[] buf, int len)
    {
        if (!TryReadPacket(frame, out var info, out var to, out var data)) return null;
        bool modified = false;
        if (!ConsumeHop(info, ref modified)) return null;

        string rawTo = JoinBlocks(to);
        if (data.Length >= MaxInternalPackageData || BannedAddresses.ContainsKey(rawTo))
        {
            Dbg($"Forwarding data refused for {rawTo}");
            return null;
        }

        var target = TryRouteLocal(rawTo, out var peer) ? peer : _external;
        target?.Send(FrameBytes(frame, modified, buf, len));
        return target;
    }
}
