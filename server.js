
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const wss = new WebSocket.Server({ port: PORT });

const ROOM = "oneclick_voice_global_main_room";
const members = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, exclude = null) {
  for (const client of members.keys()) {
    if (client !== exclude) send(client, data);
  }
}

wss.on("connection", (ws) => {
  ws.id = crypto.randomUUID();
  ws.memberNo = null;

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (data.type === "join") {
      if (members.has(ws)) return;

      ws.memberNo = members.size + 1;
      members.set(ws, {
        peerId: data.peerId,
        memberNo: ws.memberNo
      });

      // Give newcomer existing members
      send(ws, {
        type: "room-members",
        members: [...members.entries()]
          .filter(([client]) => client !== ws)
          .map(([, m]) => m)
      });

      // Tell others newcomer arrived
      broadcast({
        type: "join",
        sender: data.peerId,
        peerId: data.peerId,
        memberNo: ws.memberNo
      }, ws);

      return;
    }

    // Relay WebRTC signaling only to intended peer
    if (data.target) {
      for (const [client, m] of members) {
        if (client !== ws && m.peerId === data.target) {
          send(client, {
            ...data,
            sender: members.get(ws)?.peerId
          });
          break;
        }
      }
    }

    // Mute/leave notifications
    if (data.type === "mute-status") {
      broadcast({
        ...data,
        sender: members.get(ws)?.peerId
      }, ws);
    }
  });

  ws.on("close", () => {
    const member = members.get(ws);
    if (!member) return;

    members.delete(ws);

    broadcast({
      type: "leave",
      sender: member.peerId
    });

    // Compact numbering for future members
    let n = 1;
    for (const m of members.values()) {
      m.memberNo = n++;
    }
  });
});

console.log("OneClick Voice Global Room backend running");
