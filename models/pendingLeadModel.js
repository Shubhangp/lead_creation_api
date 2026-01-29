const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
  ScanCommand
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'pending_leads';

class PendingLead {
  /**
   * Create or update pending lead entry
   * One entry per lead with array of pending lenders
   */
  static async createOrUpdate(leadId, lenderNames, leadData, distributionType = 'immediate') {
    try {
      // Check if entry already exists
      const existing = await this.findByLeadId(leadId);

      if (existing) {
        // Merge lender names (avoid duplicates)
        const mergedLenders = [...new Set([...existing.lenderNames, ...lenderNames])];
        
        return await this.updateById(existing.pendingLeadId, {
          lenderNames: mergedLenders,
          leadData: leadData // Update with latest data
        });
      }

      // Create new entry
      const item = {
        pendingLeadId: uuidv4(),
        leadId: leadId,
        lenderNames: lenderNames, // Array of lender names
        distributionType: distributionType,
        leadData: leadData,
        status: 'pending',
        attempts: 0,
        lastProcessedAt: null,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      await docClient.send(new PutCommand({
        TableName: TABLE_NAME,
        Item: item
      }));

      return item;
    } catch (error) {
      console.error('Error in createOrUpdate:', error);
      throw error;
    }
  }

  /**
   * Remove lenders from the pending list after successful send
   * Delete entry if no lenders remain
   */
  static async removeLenders(leadId, lendersToRemove) {
    try {
      const existing = await this.findByLeadId(leadId);
      
      if (!existing) {
        console.log(`No pending entry found for leadId: ${leadId}`);
        return null;
      }

      // Filter out the lenders that were successfully sent
      const remainingLenders = existing.lenderNames.filter(
        lender => !lendersToRemove.includes(lender)
      );

      if (remainingLenders.length === 0) {
        // No more pending lenders - delete the entry
        await this.deleteById(existing.pendingLeadId);
        console.log(`Deleted pending entry for leadId: ${leadId} - all lenders processed`);
        return null;
      }

      // Update with remaining lenders
      const updated = await this.updateById(existing.pendingLeadId, {
        lenderNames: remainingLenders
      });

      console.log(`Updated pending entry for leadId: ${leadId} - ${remainingLenders.length} lenders remaining`);
      return updated;
    } catch (error) {
      console.error('Error in removeLenders:', error);
      throw error;
    }
  }

  /**
   * Find by leadId (single entry per lead)
   */
  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId },
      Limit: 1
    }));

    return result.Items?.[0] || null;
  }

  /**
   * Find by ID
   */
  static async findById(pendingLeadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { pendingLeadId }
    }));

    return result.Item || null;
  }

  /**
   * Get all pending leads (for continuous processing)
   * Returns in batches for efficient processing
   */
  static async getAllPending(limit = 500, lastEvaluatedKey = null) {
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': 'pending'
      },
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey,
      count: result.Count || 0
    };
  }

  /**
   * Get pending leads that need processing (haven't been processed recently)
   * This prevents processing the same leads too frequently
   */
  static async getReadyForProcessing(cooldownMinutes = 5, limit = 500) {
    const cooldownTime = new Date(Date.now() - cooldownMinutes * 60 * 1000).toISOString();
    
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: '#status = :status',
      FilterExpression: 'attribute_not_exists(lastProcessedAt) OR lastProcessedAt < :cooldownTime',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': 'pending',
        ':cooldownTime': cooldownTime
      },
      Limit: limit
    }));

    return result.Items || [];
  }

  /**
   * Update by ID
   */
  static async updateById(pendingLeadId, updates) {
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    // Always update the updatedAt timestamp
    updates.updatedAt = new Date().toISOString();

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { pendingLeadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  /**
   * Mark as processed (update lastProcessedAt timestamp)
   */
  static async markAsProcessed(pendingLeadId) {
    return await this.updateById(pendingLeadId, {
      lastProcessedAt: new Date().toISOString(),
      attempts: 0 // Reset attempts after successful processing
    });
  }

  /**
   * Increment attempt counter
   */
  static async incrementAttempts(pendingLeadId) {
    const item = await this.findById(pendingLeadId);
    if (!item) return null;

    return await this.updateById(pendingLeadId, {
      attempts: (item.attempts || 0) + 1,
      lastProcessedAt: new Date().toISOString()
    });
  }

  /**
   * Delete by ID
   */
  static async deleteById(pendingLeadId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { pendingLeadId }
    }));

    return { deleted: true };
  }

  /**
   * Delete by leadId (cleanup)
   */
  static async deleteByLeadId(leadId) {
    const existing = await this.findByLeadId(leadId);
    if (existing) {
      await this.deleteById(existing.pendingLeadId);
      return { deleted: true };
    }
    return { deleted: false };
  }

  /**
   * Get statistics
   */
  static async getStats() {
    const result = await docClient.send(new ScanCommand({
      TableName: TABLE_NAME,
      Select: 'ALL_ATTRIBUTES'
    }));

    const items = result.Items || [];
    
    const stats = {
      totalEntries: items.length,
      totalPendingLenders: 0,
      byDistributionType: {
        immediate: 0,
        delayed: 0
      },
      lenderBreakdown: {}
    };

    items.forEach(item => {
      // Count pending lenders
      stats.totalPendingLenders += (item.lenderNames || []).length;

      // Count by distribution type
      const type = item.distributionType || 'immediate';
      stats.byDistributionType[type]++;

      // Count by lender
      (item.lenderNames || []).forEach(lender => {
        stats.lenderBreakdown[lender] = (stats.lenderBreakdown[lender] || 0) + 1;
      });
    });

    return stats;
  }

  /**
   * Get count of pending entries
   */
  static async getPendingCount() {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': 'pending'
      },
      Select: 'COUNT'
    }));

    return result.Count || 0;
  }

  /**
   * Cleanup old completed entries (optional maintenance)
   */
  static async cleanupOldEntries(daysOld = 7) {
    const cutoffDate = new Date(Date.now() - daysOld * 24 * 60 * 60 * 1000).toISOString();
    
    let deletedCount = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        FilterExpression: '#status = :completed AND updatedAt < :cutoffDate',
        ExpressionAttributeNames: {
          '#status': 'status'
        },
        ExpressionAttributeValues: {
          ':completed': 'completed',
          ':cutoffDate': cutoffDate
        },
        Limit: 100
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      
      // Delete found items
      for (const item of result.Items || []) {
        await this.deleteById(item.pendingLeadId);
        deletedCount++;
      }

      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return deletedCount;
  }
}

module.exports = PendingLead;