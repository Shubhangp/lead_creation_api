const { PutCommand, GetCommand, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const { docClient, TABLE_NAME } = require("../dynamodb");

const buildTTL = () => Math.floor(Date.now() / 1000) + 90 * 24 * 60 * 60;

const parseRCSPayload = (body) => {
  const now = new Date().toISOString();

  let messageType = "unknown";
  let messageText = null;
  let mediaUrl = null;

  if (body.message) {
    if (body.message.text) {
      messageType = "text";
      messageText = body.message.text;
    } else if (body.message.contentInfo) {
      const ct = body.message.contentInfo.contentType || "";
      messageType = ct.startsWith("image") ? "image" : ct.startsWith("video") ? "video" : "file";
      mediaUrl = body.message.contentInfo.fileUrl || null;
    } else if (body.message.location) {
      messageType = "location";
    } else if (body.message.richCard) {
      messageType = "richCard";
    }
  }

  const receivedAt = now;
  const datePartition = `date#${receivedAt.slice(0, 10)}`; // "date#2024-01-15"

  return {
    eventId: uuidv4(),
    messageId: body.messageId || body.message_id || uuidv4(),
    eventType: body.eventType || body.event_type || "UNKNOWN",
    from: body.from || body.senderPhoneNumber || null,
    to: body.to || body.agentId || null,
    timestamp: body.sendTime || body.timestamp || now,
    receivedAt,
    datePartition,
    rawPayload: JSON.stringify(body),
    messageType,
    messageText,
    mediaUrl,
    status: body.deliveryReport?.status || (body.readReport ? "READ" : "RECEIVED"),
    ttl: buildTTL(),
  };
};

// ─── Model Methods ────────────────────────────────────────────────────────────

/** Save a new RCS webhook event */
const saveEvent = async (body) => {
  const item = parseRCSPayload(body);
  await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
  return item;
};

/** Get a single event by eventId + messageId (direct key lookup — cheapest) */
const getEvent = async (eventId, messageId) => {
  const result = await docClient.send(
    new GetCommand({ TableName: TABLE_NAME, Key: { eventId, messageId } })
  );
  return result.Item || null;
};

const listEventsByDate = async (date, limit = 20) => {
  const isoDate = date || new Date().toISOString().slice(0, 10);
  const partition = `date#${isoDate}`;

  const result = await docClient.send(
    new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "datePartition-receivedAt-index",
      KeyConditionExpression: "datePartition = :dp",
      ExpressionAttributeValues: { ":dp": partition },
      ScanIndexForward: false, // newest first
      Limit: limit,
    })
  );

  return result.Items || [];
};

const listEventsByPhone = async (phoneNumber, limit = 20) => {
  const result = await docClient.send(
    new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "from-receivedAt-index",
      KeyConditionExpression: "#from = :phone",
      ExpressionAttributeNames: { "#from": "from" }, // 'from' is reserved in DynamoDB
      ExpressionAttributeValues: { ":phone": phoneNumber },
      ScanIndexForward: false, // newest first
      Limit: limit,
    })
  );

  return result.Items || [];
};

module.exports = {
  saveEvent,
  getEvent,
  listEventsByDate,
  listEventsByPhone,
  parseRCSPayload,
  TABLE_NAME,
};