import ws from "k6/ws";
import { check } from "k6";
import log from "./logger.js";

export default function () {
  const url = "ws://0.0.0.0:8000/ws";
  const params = { tags: { my_tag: "hello" } };
  const message = `VU ${__VU} says hello!`;
  const res = ws.connect(url, params, function (socket) {
    socket.on("open", function open() {
      socket.send("this is a test message");
      socket.setInterval(function send_message() {
        //log("Sending message...");
        socket.send(message);
      }, 200);
    });
    socket.on("message", (data) => log(data));
    socket.on("close", () => log("disconnected"));
    socket.setTimeout(function () {
      log("5 seconds passed, closing the socket");
      socket.close();
    }, 20000);
  });

  check(res, { "status is 101": (r) => r && r.status === 101 });
}
