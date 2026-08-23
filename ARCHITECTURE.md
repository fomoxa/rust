# Kiến trúc fomoxa-rust

Tài liệu này tóm tắt các tầng chức năng bên trong crate `fomoxa-net` (thư mục
`src/`), cách chúng giao tiếp với nhau, và quan hệ phụ thuộc giữa các module.
Đây là tài liệu đọc nhanh cho người sửa code trong repo này — quyền quyết định
về hành vi giao thức vẫn thuộc về
[protocol specification](https://github.com/fomoxa/specification)
như README đã nêu.

## Vị trí trong 3 tầng lớn của Fomoxa

README đã mô tả 3 tầng tổng quát: **codec** (sinh bởi `fomoxac`, ngoài crate
này) → **runtime** (chính crate này) → **transport** (`src/transport/`).
Tài liệu này đi sâu vào bên trong tầng **runtime**, vì đó là phần chứa logic
phức tạp nhất: framing, bắt tay schema, heartbeat, và vòng lặp tick không
block.

## Các tầng chức năng

Từ thấp lên cao (tầng dưới không biết gì về tầng trên):

| Tầng | File | Vai trò |
|---|---|---|
| Transport | `src/transport/mod.rs`, `tcp.rs`, `udp.rs` | Trait `Transport` (một peer) và `ServerTransport` (một listener) — I/O thô, non-blocking, không biết gì về Fomoxa. TCP là `TransportKind::Stream` (byte liên tục, cần tự tách khung); UDP là `TransportKind::Message` (mỗi `recv` trả về đúng một datagram). |
| Frame | `src/frame.rs` | Định dạng khung nhị phân: `Data`, `Probe`, `Ack`, `Handshake`. Cung cấp `encode_*`/`decode`/`decode_packet` thuần túy, và `StreamDecoder` để ghép các byte TCP rời rạc thành từng khung hoàn chỉnh. |
| Schema | `src/schema.rs` | `Schema`/`MessageSchema`: fingerprint của toàn schema và của từng message, cùng danh sách fingerprint tiền tố (`prefixes`) dùng để so khớp schema từng phần giữa hai phía. |
| Handshake | `src/handshake.rs` | Giao thức bắt tay thuần túy (không I/O): mã hoá/giải mã `Hello`, `Verdict`, `Query`, `Reply`; hàm `decide()` so hai schema và quyết định Accept / Reject / Query thêm thông tin. |
| Event | `src/event.rs` | Kiểu `Event` mà tầng ứng dụng nhận được (`Connected`, `Ready`, `Message`, `Probe`, `Ack`, `Disconnected`, `HandshakeFailed`), và `EventSink` — bộ đệm gộp nhiều event của nhiều peer trong một tick, dùng arena buffer để tránh cấp phát cho từng payload. |
| Session | `src/session.rs` | Máy trạng thái thuần túy (`Handshaking → Ready → Closed`) cho **một** kết nối. Nhận vào một `Frame` hoặc một mốc thời gian (`tick`), trả ra `Reaction { out, event }` — hoàn toàn không đụng tới transport. |
| Connection | `src/connection.rs` | Lớp keo nối `Session` với `Transport` thật: `Wire` (transport + hàng đợi gửi dở `Outbox` + trạng thái chết), `Core` (wire + bộ giải mã khung + session), và `Connection` — API công khai cho client (một peer). |
| Server | `src/server.rs` | Quản lý nhiều `Peer` (mỗi peer là một `Core` dùng lại từ `connection.rs`) trên một `ServerTransport`. Chấp nhận kết nối mới mỗi tick, tick từng peer, dọn peer đã kết thúc. |

## Các tầng giao tiếp với nhau như thế nào

Không có thread nền, không có async — mọi thứ được thúc đẩy bởi lời gọi
`tick(now)` tường minh từ ứng dụng. Luồng dữ liệu trong một tick:

**Chiều nhận (recv):**
```
Transport::recv(buf)                              (bytes thô, non-blocking)
   -> StreamDecoder (nếu TCP) ghép byte thành khung, hoặc decode_packet (nếu UDP)
   -> frame::decode trả về Frame<'_>
   -> Session::on_frame(frame, now) -> Reaction { out, event }
   -> Core: nếu có `out` thì frame::encode_* rồi Wire::send_control (gửi ngay)
          nếu có `event` thì EventSink::push(peer, event)
   -> Connection/Server trả về Events<'_>/PeerEvents<'_> cho ứng dụng đọc
```

**Chiều gửi (send) do ứng dụng chủ động gọi:**
```
Connection::send(id, payload) / Server::send(peer, id, payload)
   -> Core::send: kiểm tra Session::is_ready(), frame::encode_data
   -> Wire::send_message -> Transport::send (hoặc xếp vào Outbox nếu WouldBlock)
```

**Chiều thời gian (mỗi tick, không phụ thuộc I/O):**
```
Session::tick(now) -> có thể sinh Out::Probe (heartbeat) hoặc phát hiện
timeout bắt tay / timeout heartbeat -> Reaction { out, event }
```

Điểm mấu chốt: `Session` không bao giờ gọi trực tiếp vào `Transport`. Nó chỉ
nhận `Frame`/thời gian và trả ra ý định (`Reaction`); `Core` là nơi duy nhất
biến ý định đó thành byte thật gửi qua `Wire`/`Transport`. Nhờ vậy `session.rs`
có thể test được hoàn toàn bằng unit test thuần, không cần transport giả lập.

`handshake.rs` và `schema.rs` cũng là các module thuần hàm (pure functions) —
chúng không giữ trạng thái, không I/O; `Session` gọi vào chúng để mã hoá/giải
mã payload bắt tay và ra quyết định Accept/Reject/Query.

## Quan hệ phụ thuộc giữa các module

Dựa trên các câu `use crate::...` thực tế trong mã nguồn:

```
transport   (không phụ thuộc module nào khác trong crate)
frame       (không phụ thuộc module nào khác trong crate)

schema  <--> handshake     (phụ thuộc lẫn nhau:
                             schema dùng handshake::MAX_MESSAGES để giới hạn
                             số message trong một Schema;
                             handshake dùng schema::Schema để so khớp/trả lời
                             Hello/Query)

event   --depends on-->    handshake      (Event::HandshakeFailed bọc
                                            handshake::HandshakeFailure)

session --depends on-->    event, frame, handshake, schema
                            (máy trạng thái thuần, ghép frame vào giao thức
                             bắt tay + schema, sinh ra Event)

connection --depends on--> event, frame, schema, session, transport
                            (Core/Wire nối Session vào Transport thật)

server  --depends on-->    connection (dùng lại Core), event, schema,
                            session, transport
                            (quản lý tập hợp nhiều Core, mỗi Core là một peer)
```

Sơ đồ tầng (dưới lên trên, tầng trên dùng tầng dưới, không có chiều ngược
lại ngoại trừ cặp `schema`/`handshake`):

```
        Server (nhiều peer)
             |
        Connection (một peer)
             |
           Core / Wire   <-- điểm nối Session với Transport
          /        \
     Session       Transport (TCP/UDP)
      /   \
Handshake  Frame
    |
  Schema
      \
     Event  (chỉ mượn HandshakeFailure)
```

## Ghi chú thiết kế đáng chú ý

- **`Session` là lõi tách biệt I/O**: toàn bộ logic giao thức (bắt tay,
  heartbeat, chuyển trạng thái) nằm trong `session.rs` và không biết gì về
  socket. Đây là ranh giới kiểm thử quan trọng nhất trong crate.
- **`Core` (trong `connection.rs`) được dùng lại bởi cả `Connection` (client,
  1 peer) và `Server` (nhiều peer)** — mỗi peer trên server chỉ là một `Core`
  gắn thêm `PeerId`. Không có code trùng lặp giữa client và server cho phần
  đọc/ghi khung hay chạy máy trạng thái.
- **`TransportKind::Stream` vs `Message`** quyết định `Core` có cần
  `StreamDecoder` để ghép byte hay không — đây là điểm khác biệt duy nhất
  giữa đường đi của TCP và UDP bên trong `connection.rs`.
- **Không có reconnect tự động**: như đoạn bạn đang chọn trong README đã nêu,
  quyết định kết nối lại là của tầng ứng dụng, không phải của transport hay
  runtime.
