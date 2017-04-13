function WebSocketTest()
         {



               // Let us open a web socket
               var ws = new WebSocket("ws://localhost:9000/echo");

               ws.onopen = function()
               {
                  // Web Socket is connected, send data using send()run
                  //alert($('#texttokafka').val());
                  ws.send($('#texttokafka').val());
                  //alert("Message is sent...");
               };

               ws.onmessage = function (evt)
               {
                  var received_msg = evt.data;
                  //alert(received_msg)
                  document.getElementById("demo").innerHTML = received_msg;
               };

               ws.onclose = function()
               {
                  // websocket is closed.
                  //alert("Connection is closed...");
               };

         }