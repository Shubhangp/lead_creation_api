const { PutCommand, GetCommand, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const { docClient} = require("../dynamodb");

const buildTTL = () => Math.floor(Date.now() / 1000) + 90 * 24 * 60 * 60;

const TableName = "rcs_webhook";

// Extract the event timestamp regardless of which key Tata uses
const extractEventTimestamp = (entity) =>
  entity.failed     ||
  entity.sent       ||
  entity.delivered  ||
  entity.read       ||
  entity.received   ||
  null;

// ─── Parse Tata Telecom RCS Payload ──────────────────────────────────────────
const parseRCSPayload = (body) => {
  const now = new Date().toISOString();
  const entity = body.entity || {};

  const receivedAt = now;
  const datePartition = `date#${receivedAt.slice(0, 10)}`;

  return {
    // ── Keys ────────────────────────────────────────────────────────────────
    messageId:        entity.messageId || uuidv4(),       // PK
    receivedAt,                                            // SK

    // ── Top-level fields ────────────────────────────────────────────────────
    userPhoneNumber:  body.userPhoneNumber  || null,
    entityType:       body.entityType       || null,       // "USER_EVENT"
    templateName:     body.templateName     || null,
    traceId:          body.traceId          || null,

    // ── entity fields ───────────────────────────────────────────────────────
    eventType:        entity.eventType      || "UNKNOWN",
    eventTimestamp:   extractEventTimestamp(entity),       // failed/sent/delivered/read

    // ── Error fields (MESSAGE_FAILED only) ──────────────────────────────────
    errorCode:        entity.errorCode        || null,
    errorDescription: entity.errorDescription || null,

    // ── Metadata ────────────────────────────────────────────────────────────
    datePartition,
    rawPayload: JSON.stringify(body),
    ttl: buildTTL(),
  };
};

// ─── Model Methods ────────────────────────────────────────────────────────────

/** Save incoming RCS webhook event */
const saveEvent = async (body) => {
  const item = parseRCSPayload(body);
  await docClient.send(new PutCommand({ TableName, Item: item }));
  return item;
};

/** Get a single event by messageId + receivedAt */
const getEvent = async (messageId, receivedAt) => {
  const result = await docClient.send(
    new GetCommand({ TableName, Key: { messageId, receivedAt } })
  );
  return result.Item || null;
};

/**
 * Query events by date — GSI 2 (datePartition-receivedAt-index)
 * Defaults to today. No Scan — reads only that day's partition.
 */
const listEventsByDate = async (date, limit = 20) => {
  const isoDate = date || new Date().toISOString().slice(0, 10);
  const result = await docClient.send(
    new QueryCommand({
      TableName,
      IndexName: "datePartition-receivedAt-index",
      KeyConditionExpression: "datePartition = :dp",
      ExpressionAttributeValues: { ":dp": `date#${isoDate}` },
      ScanIndexForward: false, // newest first
      Limit: limit,
    })
  );
  return result.Items || [];
};

/**
 * Query events by phone number — GSI 1 (userPhoneNumber-receivedAt-index)
 * Reads only that user's partition.
 */
const listEventsByPhone = async (phoneNumber, limit = 20) => {
  const result = await docClient.send(
    new QueryCommand({
      TableName,
      IndexName: "userPhoneNumber-receivedAt-index",
      KeyConditionExpression: "userPhoneNumber = :phone",
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
};