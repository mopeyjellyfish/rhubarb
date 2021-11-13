import ws from 'k6/ws';
import { check } from 'k6';

export default function () {
  const url = 'ws://0.0.0.0:8000/ws';
  const params = { tags: { my_tag: 'hello' } };

  const res = ws.connect(url, params, function (socket) {
    socket.on('open', function open(){
        socket.send("this is a test message")
        socket.setInterval(function send_message() {
            //console.log("Sending message...")   
            socket.send("this is a test message")
          }, 1000); 
    });
    socket.on('message', (data) => console.log('Message received'));
    socket.on('close', () => console.log('disconnected'));
    socket.setTimeout(function () {
      console.log('5 seconds passed, closing the socket');
      socket.close();
    }, 20000);
  });

  check(res, { 'status is 101': (r) => r && r.status === 101 });
}
