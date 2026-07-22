package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/skip2/go-qrcode"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	moderncsqlite "modernc.org/sqlite"

	"go.mau.fi/whatsmeow"
)

func init() {
	// modernc.org/sqlite is pure-Go (no CGO). Register it as "sqlite3"
	// so whatsmeow's sqlstore can use it without mattn/go-sqlite3.
	sql.Register("sqlite3", &moderncsqlite.Driver{})
}

// ── Global state ─────────────────────────────────────────────────────────────
var mu sync.Mutex

var (
	waClient    *whatsmeow.Client
	isReady     bool
	qrCodeData  string // base64 PNG data-URL
	connecting  bool
	retryDelay  = 5 * time.Second
	dbContainer *sqlstore.Container
)

var (
	spamActive      bool
	spamCancelFn    context.CancelFunc
	spamSpeed       = 3 * time.Second
	targetChatID    string
	messageList     []string
	messagePrefix   string
	currentMsgIndex int
	messageCount    int
)

// ── DB / session init ─────────────────────────────────────────────────────────
func initDB() error {
	os.MkdirAll("session", 0o755)
	db, err := sql.Open("sqlite3", "file:session/wa.db?_foreign_keys=on&_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	c, err := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err != nil {
		return fmt.Errorf("sqlstore: %w", err)
	}
	if err = c.Upgrade(); err != nil {
		return fmt.Errorf("sqlstore upgrade: %w", err)
	}
	dbContainer = c
	return nil
}

// ── WhatsApp bağlantısı ───────────────────────────────────────────────────────
func connect() {
	mu.Lock()
	if connecting {
		mu.Unlock()
		return
	}
	connecting = true
	mu.Unlock()

	defer func() {
		mu.Lock()
		connecting = false
		mu.Unlock()
	}()

	deviceStore, err := dbContainer.GetFirstDevice()
	if err != nil {
		log.Printf("❌ Device store: %v", err)
		scheduleReconnect()
		return
	}

	cli := whatsmeow.NewClient(deviceStore, waLog.Noop)
	cli.AddEventHandler(handleEvent)

	mu.Lock()
	waClient = cli
	mu.Unlock()

	if deviceStore.ID == nil {
		// Oturum yok → QR gerekiyor
		qrChan, err := cli.GetQRChannel(context.Background())
		if err != nil {
			log.Printf("❌ QR kanalı: %v", err)
			scheduleReconnect()
			return
		}
		if err = cli.Connect(); err != nil {
			log.Printf("❌ Connect: %v", err)
			scheduleReconnect()
			return
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				png, err := qrcode.Encode(evt.Code, qrcode.Medium, 300)
				if err == nil {
					mu.Lock()
					qrCodeData = "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
					mu.Unlock()
					log.Println("📱 QR oluştu — taranmayı bekliyor")
				}
			} else {
				log.Printf("QR event: %s", evt.Event)
			}
		}
	} else {
		// Oturum var → direkt bağlan
		if err = cli.Connect(); err != nil {
			log.Printf("❌ Connect: %v", err)
			scheduleReconnect()
		}
	}
}

func handleEvent(rawEvt interface{}) {
	switch v := rawEvt.(type) {
	case *events.Connected:
		mu.Lock()
		isReady = true
		qrCodeData = ""
		retryDelay = 5 * time.Second
		mu.Unlock()
		log.Println("✅ WhatsApp BAĞLANDI — oturum kalıcı")

	case *events.Disconnected:
		mu.Lock()
		isReady = false
		mu.Unlock()
		log.Printf("🔌 Bağlantı kapandı: %v", v)
		go scheduleReconnect()

	case *events.LoggedOut:
		mu.Lock()
		isReady = false
		qrCodeData = ""
		mu.Unlock()
		log.Println("🚪 Logged out — oturum siliniyor")
		os.Remove("session/wa.db")
		time.Sleep(3 * time.Second)
		go connect()
	}
}

func scheduleReconnect() {
	mu.Lock()
	d := retryDelay
	if retryDelay < 60*time.Second {
		retryDelay = min(retryDelay*2, 60*time.Second)
	}
	mu.Unlock()
	log.Printf("🔄 %v sonra yeniden bağlanılacak...", d)
	time.Sleep(d)
	connect()
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// ── Spam döngüsü ─────────────────────────────────────────────────────────────
func runSpam(ctx context.Context) {
	idx := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("⏹ Spam durduruldu")
			return
		default:
		}

		mu.Lock()
		ready := isReady
		cli := waClient
		tgt := targetChatID
		msgs := messageList
		pfx := messagePrefix
		spd := spamSpeed
		mu.Unlock()

		if !ready || cli == nil || len(msgs) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if idx >= len(msgs) {
			idx = 0
		}

		jid, err := types.ParseJID(tgt)
		if err != nil {
			log.Printf("❌ JID parse: %v", err)
			time.Sleep(spd)
			idx++
			continue
		}

		raw := msgs[idx]
		final := raw
		if pfx != "" {
			final = pfx + "\n" + raw
		}

		// "Yazıyor..." efekti
		_ = cli.SendChatPresence(jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)
		time.Sleep(500 * time.Millisecond)

		_, err = cli.SendMessage(ctx, jid, &waProto.Message{
			Conversation: proto.String(final),
		})
		if err != nil {
			log.Printf("❌ Gönderim: %v", err)
		} else {
			log.Printf("📤 [%d/%d] %s", idx+1, len(msgs), trunc(final, 60))
		}

		mu.Lock()
		currentMsgIndex = idx
		mu.Unlock()

		idx++

		select {
		case <-ctx.Done():
			return
		case <-time.After(spd):
		}
	}
}

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func filterEmpty(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────
func indexHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, htmlPage)
}

func statusHandler(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	data := map[string]interface{}{
		"isReady":      isReady,
		"qr":           qrCodeData,
		"spamActive":   spamActive,
		"currentIndex": currentMsgIndex,
		"messageCount": messageCount,
	}
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Speed    int      `json:"speed"`
		Target   string   `json:"target"`
		Messages []string `json:"messages"`
		Prefix   string   `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, err.Error())
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if !isReady {
		jsonErr(w, "WhatsApp bağlı değil")
		return
	}
	if req.Target == "" {
		jsonErr(w, "Hedef ID boş")
		return
	}
	msgs := filterEmpty(req.Messages)
	if len(msgs) == 0 {
		jsonErr(w, "Mesaj listesi boş")
		return
	}

	// Önceki spam'i durdur
	if spamActive && spamCancelFn != nil {
		spamCancelFn()
	}

	if req.Speed < 500 {
		req.Speed = 3000
	}
	spamSpeed = time.Duration(req.Speed) * time.Millisecond
	targetChatID = req.Target
	messageList = msgs
	messagePrefix = strings.TrimSpace(req.Prefix)
	messageCount = len(msgs)
	currentMsgIndex = 0

	ctx, cancel := context.WithCancel(context.Background())
	spamCancelFn = cancel
	spamActive = true
	go runSpam(ctx)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	if spamActive && spamCancelFn != nil {
		spamCancelFn()
	}
	spamActive = false
	currentMsgIndex = 0
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func refreshQRHandler(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	isReady = false
	qrCodeData = ""
	connecting = false
	cli := waClient
	waClient = nil
	mu.Unlock()

	if cli != nil {
		go cli.Disconnect()
	}
	time.Sleep(800 * time.Millisecond)
	go connect()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func chatsHandler(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	ready := isReady
	cli := waClient
	mu.Unlock()

	if !ready || cli == nil {
		jsonErr(w, "Bağlı değil")
		return
	}

	groups, err := cli.GetJoinedGroups()
	if err != nil {
		jsonErr(w, err.Error())
		return
	}

	type chat struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	chats := make([]chat, 0, len(groups))
	for _, g := range groups {
		chats = append(chats, chat{ID: g.JID.String(), Name: g.Name})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "chats": chats})
}

func jsonErr(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": msg})
}

// ── Main ──────────────────────────────────────────────────────────────────────
func main() {
	if err := initDB(); err != nil {
		log.Fatalf("DB başlatılamadı: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/api/status", statusHandler)
	http.HandleFunc("/api/start", startHandler)
	http.HandleFunc("/api/stop", stopHandler)
	http.HandleFunc("/api/refreshqr", refreshQRHandler)
	http.HandleFunc("/api/chats", chatsHandler)

	log.Printf("🌐 http://0.0.0.0:%s", port)
	go connect()
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// ── HTML sayfası ──────────────────────────────────────────────────────────────
const htmlPage = `<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WA Spammer</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Arial,sans-serif;background:#0a0a0a;color:#eee;padding:16px}
.wrap{max-width:680px;margin:auto}
h1{text-align:center;color:#25D366;margin-bottom:16px;font-size:22px}
.qr-section{background:#111;border:2px solid #25D366;border-radius:14px;padding:20px;margin-bottom:16px;display:flex;flex-direction:column;align-items:center;gap:12px}
.qr-section h2{color:#25D366;font-size:15px;letter-spacing:.5px}
.qr-frame{background:#fff;padding:12px;border-radius:12px;width:260px;height:260px;display:flex;align-items:center;justify-content:center;box-shadow:0 0 24px #25D36644}
.qr-frame img{width:236px;height:236px;object-fit:contain}
.qr-placeholder{color:#555;font-size:13px;text-align:center;line-height:1.6}
.qr-status-text{font-size:13px;color:#aaa}
.qr-status-text.ok{color:#25D366;font-weight:bold}
.status-bar{display:flex;align-items:center;gap:10px;background:#1a1a1a;border:1px solid #333;padding:10px 14px;border-radius:8px;margin-bottom:14px}
.dot{width:11px;height:11px;border-radius:50%;flex-shrink:0}
.dot.on{background:#25D366;box-shadow:0 0 8px #25D366}
.dot.off{background:#f44;box-shadow:0 0 8px #f44}
.dot.wait{background:#fa0;box-shadow:0 0 8px #fa0}
.panel{background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:14px;margin-bottom:14px}
.panel label{display:block;color:#888;font-size:12px;margin-bottom:3px;margin-top:8px}
.panel label:first-child{margin-top:0}
input,textarea{width:100%;padding:8px 10px;border:1px solid #333;border-radius:6px;background:#0d0d0d;color:#eee;font-size:14px;resize:vertical}
textarea{font-family:monospace}
.preview-box{background:#0a1f12;border:1px solid #25D366;border-radius:6px;padding:8px 10px;font-size:13px;color:#ccc;white-space:pre-wrap;min-height:34px;margin-top:2px}
.preview-box .pfx{color:#25D366;font-weight:bold}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.btn{padding:9px 18px;border:none;border-radius:7px;font-weight:bold;cursor:pointer;font-size:14px}
.g{background:#25D366;color:#000}.r{background:#e33;color:#fff}.b{background:#0055cc;color:#fff}
.btn:disabled{opacity:.35;cursor:default}
.chat-list{background:#0d0d0d;border:1px solid #2a2a2a;border-radius:8px;max-height:140px;overflow-y:auto;margin-top:8px}
.chat-item{padding:7px 12px;border-bottom:1px solid #1a1a1a;cursor:pointer;font-size:13px}
.chat-item:hover{background:#1a1a1a}
.chat-item .n{color:#25D366}.chat-item .i{color:#555;font-size:11px;display:block}
.log-box{background:#050505;border:1px solid #222;border-radius:8px;max-height:180px;overflow-y:auto;padding:6px}
.log-item{padding:4px 8px;border-bottom:1px solid #111;font-size:12px;font-family:monospace;color:#bbb}
.loop-info{color:#25D366;font-size:12px;margin-top:6px}
.typing-anim{color:#25D366;font-style:italic;animation:blink 1s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0}}
.btn-refresh{background:#2a2a2a;color:#aaa;border:1px solid #444;border-radius:6px;padding:5px 14px;cursor:pointer;font-size:12px}
</style>
</head>
<body>
<div class="wrap">
<h1>💬 WhatsApp Spammer</h1>

<div class="qr-section">
  <h2>📷 WhatsApp Web Bağlantısı</h2>
  <div class="qr-frame" id="qrFrame">
    <div class="qr-placeholder" id="qrPlaceholder">⏳ QR yükleniyor...<br>Lütfen bekleyin</div>
  </div>
  <div class="qr-status-text" id="qrStatusText">Bağlantı kuruluyor...</div>
  <button class="btn-refresh" onclick="forceRefreshQR()">🔄 Yeniden Bağlan</button>
</div>

<div class="status-bar">
  <span class="dot off" id="dot"></span>
  <span id="statusLabel">⏳ Bağlanıyor...</span>
  <span style="margin-left:auto;font-size:11px;color:#555" id="statusSub">oturum bekleniyor</span>
</div>

<div class="panel">
  <label>⏱ Hız (ms)</label>
  <input type="number" id="speed" value="3000" min="500" step="500">

  <label>📌 Hedef Sohbet ID</label>
  <input type="text" id="target" placeholder="örn: 905551234567@c.us">

  <label>🏷 Prefix / Etiket <small style="color:#555">(boş bırakılabilir)</small></label>
  <input type="text" id="prefix" placeholder="örn: 🔔 Duyuru:" oninput="renderPreview()">

  <label>📝 Mesajlar <small style="color:#555">(her satır = 1 mesaj, sonsuz döngü)</small></label>
  <textarea id="messages" rows="5" oninput="renderPreview()">Merhaba!
Nasılsın?
Test mesajı 3
Test mesajı 4</textarea>

  <label style="color:#555">👁 Önizleme (ilk mesaj)</label>
  <div class="preview-box" id="preview">—</div>

  <div class="row">
    <button class="btn g" id="btnStart" onclick="startSpam()" disabled>▶ Başlat</button>
    <button class="btn r" id="btnStop"  onclick="stopSpam()"  disabled>⏹ Durdur</button>
    <button class="btn b" onclick="fetchChats()">🔄 Gruplar</button>
  </div>
  <div class="loop-info" id="loopInfo" style="display:none"></div>
</div>

<div class="chat-list" id="chatList" style="display:none"></div>

<div class="panel">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
    <span style="font-size:13px">📜 Log</span>
    <span class="typing-anim" id="typingBadge" style="display:none">✍️ typing...</span>
  </div>
  <div class="log-box" id="logBox"></div>
</div>

<div style="text-align:center;color:#333;font-size:11px;margin-top:8px">
  Sonsuz döngü · Typing efekti · Prefix · Kalıcı oturum
</div>
</div>

<script>
let logN=0;
function log(msg){
  const box=document.getElementById('logBox');
  if(logN===0)box.innerHTML='';
  const d=document.createElement('div');
  d.className='log-item';
  d.textContent='['+new Date().toLocaleTimeString()+'] '+msg;
  box.appendChild(d);
  box.scrollTop=box.scrollHeight;
  if(++logN>300){box.removeChild(box.firstChild);logN--;}
}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function renderPreview(){
  const pfx=document.getElementById('prefix').value.trim();
  const msg=(document.getElementById('messages').value.split('\n').find(l=>l.trim()))||'(boş)';
  const box=document.getElementById('preview');
  box.innerHTML=pfx?'<span class="pfx">'+esc(pfx)+'</span>\n'+esc(msg):esc(msg);
}
renderPreview();

let lastQr=null,wasReady=false;
setInterval(async()=>{
  try{
    const d=await fetch('/api/status').then(r=>r.json());
    const dot=document.getElementById('dot');
    const label=document.getElementById('statusLabel');
    const sub=document.getElementById('statusSub');
    if(d.isReady){
      dot.className='dot on';label.textContent='✅ Bağlı';
      sub.textContent='Oturum kalıcı — tekrar QR okutmana gerek yok';
      document.getElementById('btnStart').disabled=false;
      if(!wasReady){log('✅ WhatsApp bağlandı!');wasReady=true;}
    }else if(d.qr){
      dot.className='dot wait';label.textContent='📱 QR Bekleniyor';
      sub.textContent='WhatsApp ile tara';
      document.getElementById('btnStart').disabled=true;wasReady=false;
    }else{
      dot.className='dot off';label.textContent='⏳ Bağlanıyor...';
      sub.textContent='lütfen bekleyin';
      document.getElementById('btnStart').disabled=true;wasReady=false;
    }
    const frame=document.getElementById('qrFrame');
    const qrStatus=document.getElementById('qrStatusText');
    if(d.isReady){
      frame.innerHTML='<div style="font-size:40px">✅</div>';
      qrStatus.textContent='Bağlantı aktif — oturum kalıcı';
      qrStatus.className='qr-status-text ok';lastQr=null;
    }else if(d.qr&&d.qr!==lastQr){
      lastQr=d.qr;
      const img=document.createElement('img');img.src=d.qr;img.alt='QR';
      frame.innerHTML='';frame.appendChild(img);
      qrStatus.textContent='📱 WhatsApp → Bağlı Cihazlar → Cihaz Ekle → Tara';
      qrStatus.className='qr-status-text';
      log('📱 Yeni QR kod geldi — tarayın!');
    }else if(!d.qr&&!d.isReady&&lastQr!==null){
      lastQr=null;
      frame.innerHTML='<div class="qr-placeholder">⏳ Yeni QR bekleniyor...<br>Bağlantı kuruluyor</div>';
      qrStatus.textContent='Bağlanıyor...';qrStatus.className='qr-status-text';
    }
    if(d.spamActive){
      document.getElementById('typingBadge').style.display='inline';
      document.getElementById('loopInfo').style.display='block';
      document.getElementById('loopInfo').textContent=
        '✍️ ['+(d.currentIndex+1)+'/'+d.messageCount+'] — sonsuz döngü aktif';
      document.getElementById('btnStop').disabled=false;
      document.getElementById('btnStart').disabled=true;
    }else{
      document.getElementById('typingBadge').style.display='none';
      document.getElementById('loopInfo').style.display='none';
    }
  }catch(e){}
},2500);

async function startSpam(){
  const speed=parseInt(document.getElementById('speed').value)||3000;
  const target=document.getElementById('target').value.trim();
  const prefix=document.getElementById('prefix').value.trim();
  const messages=document.getElementById('messages').value.split('\n').filter(l=>l.trim());
  if(!target){alert('Hedef ID girin!');return;}
  if(!messages.length){alert('En az 1 mesaj girin!');return;}
  const d=await fetch('/api/start',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({speed,target,messages,prefix})}).then(r=>r.json());
  if(d.success){
    log('🚀 Spam başlatıldı — sonsuz döngü + typing efekti');
    document.getElementById('btnStart').disabled=true;
    document.getElementById('btnStop').disabled=false;
  }else log('❌ '+d.error);
}
async function stopSpam(){
  await fetch('/api/stop',{method:'POST'});
  log('⏹ Spam durduruldu');
  document.getElementById('btnStart').disabled=false;
  document.getElementById('btnStop').disabled=true;
}
async function fetchChats(){
  const d=await fetch('/api/chats').then(r=>r.json());
  const box=document.getElementById('chatList');
  if(!d.success){log('❌ '+d.error);return;}
  box.style.display='block';
  box.innerHTML=d.chats&&d.chats.length
    ?d.chats.map(c=>'<div class="chat-item" onclick="selChat(\''+c.id+'\')"><span class="n">'+esc(c.name)+'</span><span class="i">'+c.id+'</span></div>').join('')
    :'<div class="chat-item" style="color:#555">Grup bulunamadı</div>';
}
function selChat(id){document.getElementById('target').value=id;log('📌 Hedef: '+id);}
function forceRefreshQR(){
  lastQr=null;
  fetch('/api/refreshqr',{method:'POST'});
  log('🔄 Yeniden bağlanma istendi...');
  document.getElementById('qrFrame').innerHTML='<div class="qr-placeholder">⏳ Yeniden bağlanıyor...</div>';
}
</script>
</body>
</html>`
