#include "WebsocketHandler.hpp"

#ifndef TAG
#define TAG "WebsocketHandler"
#endif
namespace httpsserver {

/**
 * @brief Dump the content of the WebSocket frame for debugging.
 * @param [in] frame The frame to dump.
 */
static void dumpFrame(WebsocketFrame frame) {
  std::string opcode = std::string("Unknown");
  switch(frame.opCode) {
    case WebsocketHandler::OPCODE_BINARY: opcode = std::string("BINARY"); break;
    case WebsocketHandler::OPCODE_CONTINUE: opcode = std::string("CONTINUE"); break;
    case WebsocketHandler::OPCODE_CLOSE: opcode = std::string("CLOSE"); break;
    case WebsocketHandler::OPCODE_PING: opcode = std::string("PING"); break;
    case WebsocketHandler::OPCODE_PONG: opcode = std::string("PONG"); break;
    case WebsocketHandler::OPCODE_TEXT: opcode = std::string("TEXT"); break;
  }
  ESP_LOGI(
    TAG,
    "Fin: %d, OpCode: %d (%s), Mask: %d, Len: %d",
    (int)frame.fin,
    (int)frame.opCode,
    opcode.c_str(),
    (int)frame.mask,
    (int)frame.len
  );
}

WebsocketHandler::WebsocketHandler() {
  _con = nullptr;
  _receivedClose = false;
  _sentClose = false;
}

WebsocketHandler::~WebsocketHandler() {

} // ~WebSocketHandler()


/**
* @brief The default onClose handler.
* If no over-riding handler is provided for the "close" event, this method is called.
*/
void WebsocketHandler::onClose() {
  HTTPS_LOGD("WebsocketHandler close()");
}

/**
* @brief The default onData handler.
* If no over-riding handler is provided for the "message" event, this method is called.
* A particularly useful pattern for using onMessage is:
* ```
* std::stringstream buffer;
* buffer << pWebSocketInputRecordStreambuf;
* ```
* This will read the whole message into the string stream.
*/
void WebsocketHandler::onMessage(WebsocketInputStreambuf* pWebsocketInputStreambuf) { //, Websocket *pWebSocket) {
  HTTPS_LOGD("WebsocketHandler onMessage()");
}


/**
* @brief The default onError handler.
* If no over-riding handler is provided for the "error" event, this method is called.
*/
void WebsocketHandler::onError(std::string error) {
  HTTPS_LOGD("WebsocketHandler onError()");
}

void WebsocketHandler::initialize(ConnectionContext * con) {
  _con = con;
}

void WebsocketHandler::loop() {
  if(read() < 0) {
    close();
  }
}

int WebsocketHandler::read() {
  WebsocketFrame frame;
  int length = _con->readBuffer((uint8_t*)&frame, sizeof(frame));
  HTTPS_LOGD("Websocket: Read %d bytes", length);
  if(length == 0) 
    return 0;
  else if (length != sizeof(frame)) {
    HTTPS_LOGE("Websocket read error");
    //_con->closeConnection();
    return -1;
  }
  dumpFrame(frame);

  // The following section parses the WebSocket frame.
  uint32_t payloadLen = 0;
  uint8_t  mask[4];
  if (frame.len < 126) {
    payloadLen = frame.len;
  } else if (frame.len == 126) {
    uint16_t tempLen;
    _con->readBuffer((uint8_t*)&tempLen, sizeof(tempLen));
    payloadLen = ntohs(tempLen);
  } else if (frame.len == 127) {
    uint64_t tempLen;
    _con->readBuffer((uint8_t*)&tempLen, sizeof(tempLen));
    payloadLen = ntohl((uint32_t)tempLen);
  }
  if (frame.mask == 1) {
    _con->readBuffer(mask, sizeof(mask));
  }

  if (payloadLen == 0) {
    HTTPS_LOGW("WS payload not present");
  } else {
    HTTPS_LOGI("WS payload: length=%d", payloadLen);
  }

  switch(frame.opCode) {
    case OPCODE_TEXT:
    case OPCODE_BINARY: {
      HTTPS_LOGD("Creating Streambuf");
      WebsocketInputStreambuf streambuf(_con, payloadLen, frame.mask==1?mask:nullptr);
      HTTPS_LOGD("Calling onMessage");
      onMessage(&streambuf);
      HTTPS_LOGD("Discarding Streambuf");
      streambuf.discard();
      break;
    }

    case OPCODE_CLOSE: {  // If the WebSocket operation code is close then we are closing the connection.
      _receivedClose = true;
      onClose();
      //close(); // Close the websocket.
      return -1;
      break;
    }

    case OPCODE_CONTINUE: {
      break;
    }

    case OPCODE_PING:
    case OPCODE_PONG: {
      // Control frames carry up to 125 bytes that have to leave the buffer with the frame
      uint8_t payload[125];
      if (payloadLen > sizeof(payload) || _con->readBuffer(payload, payloadLen) != payloadLen) {
        HTTPS_LOGE("Websocket control frame read error");
        return -1;
      }
      if (frame.opCode == OPCODE_PING) {
        if (frame.mask == 1) {
          for (uint32_t i = 0; i < payloadLen; i++) {
            payload[i] ^= mask[i % 4];
          }
        }
        WebsocketFrame pong;
        pong.fin    = 1;
        pong.rsv1   = 0;
        pong.rsv2   = 0;
        pong.rsv3   = 0;
        pong.opCode = OPCODE_PONG;
        pong.mask   = 0;
        pong.len    = payloadLen;
        if (_con->canWriteData()) {
          _con->writeBuffer((uint8_t *)&pong, sizeof(pong));
          if (payloadLen > 0) {
            _con->writeBuffer(payload, payloadLen);
          }
        }
      }
      break;
    }

    default: {
        HTTPS_LOGW("WebSocketReader: Unknown opcode: %d", frame.opCode);
      break;
    }
  } // Switch opCode
  return 0;
}  // Websocket::read

/**
 * @brief Close the Web socket
 * @param [in] status The code passed in the close request.
 * @param [in] message A clarification message on the close request.
 */
void WebsocketHandler::close(uint16_t status, std::string message) {
  HTTPS_LOGD("Websocket close()");

  _sentClose = true;              // Flag that we have sent a close request.

  WebsocketFrame frame;           // Build the web socket frame indicating a close request.
  frame.fin    = 1;
  frame.rsv1   = 0;
  frame.rsv2   = 0;
  frame.rsv3   = 0;
  frame.opCode = OPCODE_CLOSE;
  frame.mask   = 0;
  frame.len    = message.length() + 2;
  int rc = _con->writeBuffer((uint8_t *)&frame, sizeof(frame));

  if (rc > 0) {
    uint16_t netStatus = htons(status);
    rc = _con->writeBuffer((byte *) &netStatus, 2);
  }

  if (rc > 0) {
    _con->writeBuffer((byte *) message.data(), message.length());
  }
} // Websocket::close

/**
 * @brief Send data down the web socket
 * See the WebSocket spec (RFC6455) section "6.1 Sending Data".
 * We build a WebSocket frame, send the frame followed by the data.
 * @param [in] data The data to send down the WebSocket.
 * @param [in] sendType The type of payload.  Either SEND_TYPE_TEXT or SEND_TYPE_BINARY.
 */
bool WebsocketHandler::send(std::string data, uint8_t sendType) {
  HTTPS_LOGD(">> Websocket.send(): length=%d", data.length());
  WebsocketFrame frame;
  frame.fin    = 1;
  frame.rsv1   = 0;
  frame.rsv2   = 0;
  frame.rsv3   = 0;
  frame.opCode = sendType==SEND_TYPE_TEXT?OPCODE_TEXT:OPCODE_BINARY;
  frame.mask   = 0;
  bool ok;
  if (data.length() < 126) {
    frame.len = data.length();
    ok = _con->writeBuffer((uint8_t *)&frame, sizeof(frame)) == sizeof(frame);
  } else {
    frame.len = 126;
    ok = _con->writeBuffer((uint8_t *)&frame, sizeof(frame)) == sizeof(frame);
    uint16_t net_len = htons((uint16_t)data.length());
    ok = ok && _con->writeBuffer((uint8_t *)&net_len, sizeof(uint16_t)) == sizeof(uint16_t);  // Convert to network byte order from host byte order
  }
  ok = ok && _con->writeBuffer((uint8_t*)data.data(), data.length()) == data.length();
  HTTPS_LOGD("<< Websocket.send()");
  return ok;
} // Websocket::send


/**
 * @brief Send data down the web socket
 * See the WebSocket spec (RFC6455) section "6.1 Sending Data".
 * We build a WebSocket frame, send the frame followed by the data.
 * @param [in] data The data to send down the WebSocket.
 * @param [in] sendType The type of payload.  Either SEND_TYPE_TEXT or SEND_TYPE_BINARY.
 */
bool WebsocketHandler::send(uint8_t* data, uint16_t length, uint8_t sendType) {
  HTTPS_LOGD(">> Websocket.send(): length=%d", length);
  WebsocketFrame frame;
  frame.fin    = 1;
  frame.rsv1   = 0;
  frame.rsv2   = 0;
  frame.rsv3   = 0;
  frame.opCode = sendType==SEND_TYPE_TEXT?OPCODE_TEXT:OPCODE_BINARY;
  frame.mask   = 0;
  bool ok;
  if (length < 126) {
    frame.len = length;
    ok = _con->writeBuffer((uint8_t *)&frame, sizeof(frame)) == sizeof(frame);
  } else {
    frame.len = 126;
    ok = _con->writeBuffer((uint8_t *)&frame, sizeof(frame)) == sizeof(frame);
    uint16_t net_len = htons(length);
    ok = ok && _con->writeBuffer((uint8_t *)&net_len, sizeof(uint16_t)) == sizeof(uint16_t);  // Convert to network byte order from host byte order
  }
  ok = ok && _con->writeBuffer(data, length) == length;
  HTTPS_LOGD("<< Websocket.send()");
  return ok;
}  // Websocket::send

/**
 * Returns true if the socket reports writable, see the note on the declaration
 */
bool WebsocketHandler::canSend() {
  return _con->canWriteData();
}

/**
 * Drops the connection without sending a close frame
 */
void WebsocketHandler::abort() {
  _sentClose = true;
  // The connection is deleted without passing through the lost-client path, which is what calls onClose()
  onClose();
  _con->abortConnection();
}

/**
 * Returns true if the connection has been closed, either by client or server
 */
bool WebsocketHandler::closed() {
  return _receivedClose || _sentClose;
}

}
