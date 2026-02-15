const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'leads';

class Lead {
  static getDatePartition(date) {
    const d = new Date(date);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  }

  static getMonthPartitions(startDate, endDate) {
    const start = new Date(startDate);
    const end = new Date(endDate);
    const partitions = [];
    
    let current = new Date(start.getFullYear(), start.getMonth(), 1);
    const endMonth = new Date(end.getFullYear(), end.getMonth(), 1);
    
    while (current <= endMonth) {
      partitions.push(this.getDatePartition(current));
      current.setMonth(current.getMonth() + 1);
    }
    
    return partitions;
  }

  static calculateAge(dateOfBirth) {
    if (!dateOfBirth) return null;
    const dob = new Date(dateOfBirth);
    const today = new Date();
    let age = today.getFullYear() - dob.getFullYear();
    const monthDiff = today.getMonth() - dob.getMonth();
    if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < dob.getDate())) {
      age--;
    }
    return age;
  }

  static getAgeRange(age) {
    if (!age || age < 18) return 'Below 18';
    if (age >= 18 && age <= 25) return '18-25';
    if (age >= 26 && age <= 35) return '26-35';
    if (age >= 36 && age <= 45) return '36-45';
    if (age >= 46 && age <= 55) return '46-55';
    if (age >= 56 && age <= 65) return '56-65';
    return 'Above 65';
  }

  // ============================================================================
  // VALIDATION
  // ============================================================================

  static validate(data) {
    const errors = [];

    if (!data.source) errors.push('Source is required');
    if (!data.fullName) errors.push('Full name is required');
    if (!data.phone) errors.push('Phone is required');
    if (!data.email) errors.push('Email is required');
    if (!data.panNumber) errors.push('PAN number is required');

    if (data.fullName && (data.fullName.length < 1 || data.fullName.length > 100)) {
      errors.push('Full name must be between 1 and 100 characters');
    }
    if (data.firstName && (data.firstName.length < 1 || data.firstName.length > 50)) {
      errors.push('First name must be between 1 and 50 characters');
    }
    if (data.lastName && (data.lastName.length < 1 || data.lastName.length > 50)) {
      errors.push('Last name must be between 1 and 50 characters');
    }

    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}$/;
    if (data.email && !emailRegex.test(data.email)) {
      errors.push('Invalid email format');
    }

    const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
    if (data.panNumber && !panRegex.test(data.panNumber)) {
      errors.push('Invalid PAN number format (e.g., ABCDE1234F)');
    }

    if (data.age !== undefined && (data.age < 18 || data.age > 120)) {
      errors.push('Age must be between 18 and 120');
    }

    if (data.dateOfBirth) {
      const dob = new Date(data.dateOfBirth);
      if (dob > new Date()) {
        errors.push('Date of birth cannot be in the future');
      }
    }

    if (data.creditScore !== undefined && (data.creditScore < 300 || data.creditScore > 900)) {
      errors.push('Credit score must be between 300 and 900');
    }

    if (errors.length > 0) {
      const error = new Error('Validation failed');
      error.errors = errors;
      throw error;
    }
  }

  // ============================================================================
  // CRUD OPERATIONS
  // ============================================================================

  static async create(leadData) {
    this.validate(leadData);
    const existingPhone = await this.findByPhone(leadData.phone);
    if (existingPhone) {
      const error = new Error('Phone number already exists');
      error.code = 'DUPLICATE_PHONE';
      throw error;
    }
    const existingPan = await this.findByPanNumber(leadData.panNumber);
    if (existingPan) {
      const error = new Error('PAN number already exists');
      error.code = 'DUPLICATE_PAN';
      throw error;
    }

    const now = new Date().toISOString();
    const item = {
      leadId: uuidv4(),
      source: leadData.source,
      fullName: leadData.fullName,
      firstName: leadData.firstName || null,
      lastName: leadData.lastName || null,
      phone: leadData.phone,
      email: leadData.email,
      age: leadData.age || null,
      dateOfBirth: leadData.dateOfBirth ? new Date(leadData.dateOfBirth).toISOString() : null,
      gender: leadData.gender || null,
      panNumber: leadData.panNumber,
      jobType: leadData.jobType || null,
      businessType: leadData.businessType || null,
      salary: leadData.salary || null,
      creditScore: leadData.creditScore || null,
      cibilScore: leadData.cibilScore || null,
      address: leadData.address || null,
      pincode: leadData.pincode || null,
      consent: leadData.consent,
      createdAt: now,
      datePartition: this.getDatePartition(now)
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  static async findById(leadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));
    return result.Item || null;
  }

  static async findByPhone(phone) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'phone-index',
      KeyConditionExpression: 'phone = :phone',
      ExpressionAttributeValues: { ':phone': phone },
      Limit: 1
    }));
    return result.Items?.[0] || null;
  }

  static async findByPanNumber(panNumber) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'panNumber-index',
      KeyConditionExpression: 'panNumber = :panNumber',
      ExpressionAttributeValues: { ':panNumber': panNumber },
      Limit: 1
    }));
    return result.Items?.[0] || null;
  }

  static async findBySource(source, options = {}) {
    const {
      limit = 100,
      startDate,
      endDate,
      sortAscending = false,
      lastEvaluatedKey = null
    } = options;

    let keyConditionExpression = '#source = :source';
    const expressionAttributeNames = { '#source': 'source' };
    const expressionAttributeValues = { ':source': source };

    if (startDate && endDate) {
      keyConditionExpression += ' AND createdAt BETWEEN :startDate AND :endDate';
      expressionAttributeValues[':startDate'] = startDate;
      expressionAttributeValues[':endDate'] = endDate;
    } else if (startDate) {
      keyConditionExpression += ' AND createdAt >= :startDate';
      expressionAttributeValues[':startDate'] = startDate;
    } else if (endDate) {
      keyConditionExpression += ' AND createdAt <= :endDate';
      expressionAttributeValues[':endDate'] = endDate;
    }

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey || null,
      count: result.Count || 0
    };
  }

  static async findByDateRange(startDate, endDate, options = {}) {
    const { limit = 1000, lastEvaluatedKey } = options;
    
    try {
      const partitions = this.getMonthPartitions(startDate, endDate);
      const promises = partitions.map(partition => 
        this._queryByDatePartition(partition, startDate, endDate, limit)
      );
      
      const results = await Promise.all(promises);
      const allItems = results.flat();
      const items = limit ? allItems.slice(0, limit) : allItems;
      
      return {
        items,
        lastEvaluatedKey: null,
        count: items.length
      };
    } catch (error) {
      console.error('Error in findByDateRange:', error);
      throw error;
    }
  }

  static async _queryByDatePartition(partition, startDate, endDate, limit = null) {
    let items = [];
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'createdAt-index',
        KeyConditionExpression: 'datePartition = :partition AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeValues: {
          ':partition': partition,
          ':start': startDate,
          ':end': endDate
        }
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }
      if (limit) {
        params.Limit = limit;
      }

      const result = await docClient.send(new QueryCommand(params));
      items = items.concat(result.Items || []);
      lastKey = result.LastEvaluatedKey;
      
      if (limit && items.length >= limit) {
        break;
      }
    } while (lastKey);

    return items;
  }

  static async updateById(leadId, updates) {
    if (Object.keys(updates).length > 0) {
      const existingLead = await this.findById(leadId);
      if (!existingLead) {
        throw new Error('Lead not found');
      }

      const mergedData = { ...existingLead, ...updates };
      this.validate(mergedData);

      if (updates.phone && updates.phone !== existingLead.phone) {
        const existingPhone = await this.findByPhone(updates.phone);
        if (existingPhone && existingPhone.leadId !== leadId) {
          const error = new Error('Phone number already exists');
          error.code = 'DUPLICATE_PHONE';
          throw error;
        }
      }

      if (updates.panNumber && updates.panNumber !== existingLead.panNumber) {
        const existingPan = await this.findByPanNumber(updates.panNumber);
        if (existingPan && existingPan.leadId !== leadId) {
          const error = new Error('PAN number already exists');
          error.code = 'DUPLICATE_PAN';
          throw error;
        }
      }
    }

    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { leadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  static async updateByIdNoValidation(leadId, updates) {
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { leadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  static async deleteById(leadId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));
    return { deleted: true };
  }

  static async countBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: { '#source': 'source' },
      ExpressionAttributeValues: { ':source': source },
      Select: 'COUNT'
    }));
    return result.Count || 0;
  }

  // ============================================================================
  // OPTIMIZED STATISTICS FUNCTIONS WITH SOURCE BREAKDOWN
  // ============================================================================

  /**
   * Get accurate total count with SOURCE BREAKDOWN
   */
  static async getAccurateTotalCount() {
    const startTime = Date.now();
    console.log('[ANALYTICS] Getting accurate total count via GSI with source breakdown...');
    
    try {
      const sources = process.env.LEAD_SOURCES?.split(',') || [];
      
      if (sources.length === 0) {
        return await this._countViaDatePartitions();
      }

      let totalCount = 0;
      const sourceBreakdown = {}; // ✅ NEW: Track per source

      for (const source of sources) {
        const count = await this.countBySource(source.trim());
        totalCount += count;
        sourceBreakdown[source.trim()] = count; // ✅ NEW
        console.log(`  Source "${source.trim()}": ${count.toLocaleString()} records`);
      }

      const elapsed = Date.now() - startTime;
      console.log(`[ANALYTICS] ✅ Total count: ${totalCount.toLocaleString()} in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        sourceBreakdown, // ✅ NEW: Include source breakdown
        isEstimate: false,
        scannedInMs: elapsed,
        method: 'gsi-source-count',
        sources: sources.length
      };
    } catch (error) {
      console.error('Error getting accurate total count:', error);
      throw error;
    }
  }

  static async _countViaDatePartitions() {
    const startTime = Date.now();
    
    const endDate = new Date();
    const startDate = new Date();
    startDate.setFullYear(startDate.getFullYear() - 2);
    
    const partitions = this.getMonthPartitions(
      startDate.toISOString(),
      endDate.toISOString()
    );

    console.log(`  Counting ${partitions.length} monthly partitions...`);

    const countPromises = partitions.map(partition => 
      this._countFullPartition(partition)
    );

    const results = await Promise.all(countPromises);
    const totalCount = results.reduce((sum, count) => sum + count, 0);
    const elapsed = Date.now() - startTime;

    return {
      totalLogs: totalCount,
      isEstimate: false,
      scannedInMs: elapsed,
      method: 'gsi-partition-count',
      partitions: partitions.length
    };
  }

  static async _countFullPartition(partition) {
    let count = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'createdAt-index',
        KeyConditionExpression: 'datePartition = :partition',
        ExpressionAttributeValues: { ':partition': partition },
        Select: 'COUNT'
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      count += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return count;
  }

  /**
   * Get quick stats with SOURCE BREAKDOWN for date range
   */
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    try {
      if (source && !startDate) {
        const count = await this.countBySource(source);
        return {
          totalLogs: count,
          source: source,
          isEstimate: false,
          method: 'gsi-source-count'
        };
      } else if (startDate && endDate) {
        return this.getQuickStatsForDateRange(startDate, endDate); // ✅ NOW WITH SOURCE BREAKDOWN
      } else {
        console.log('[INFO] Getting accurate total count with source breakdown (real-time via GSI)');
        return this.getAccurateTotalCount(); // ✅ Already has source breakdown
      }
    } catch (error) {
      console.error('Error in getQuickStats:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Get quick count for date range WITH SOURCE BREAKDOWN
   */
  static async getQuickStatsForDateRange(startDate, endDate) {
    const startTime = Date.now();

    try {
      console.log(`[${TABLE_NAME}] Quick count with source breakdown:`, startDate, 'to', endDate);

      const sources = process.env.LEAD_SOURCES?.split(',') || [];
      
      if (sources.length === 0) {
        // Fallback without source breakdown
        return this._countDateRangeWithoutSources(startDate, endDate);
      }

      // Count each source in parallel
      const countPromises = sources.map(async (source) => {
        const trimmedSource = source.trim();
        const count = await this._countSourceInDateRange(trimmedSource, startDate, endDate);
        return { source: trimmedSource, count };
      });

      const results = await Promise.all(countPromises);
      
      // Build source breakdown
      const sourceBreakdown = {};
      let totalCount = 0;
      
      results.forEach(({ source, count }) => {
        sourceBreakdown[source] = count;
        totalCount += count;
        console.log(`  Source "${source}": ${count.toLocaleString()} records`);
      });

      const elapsed = Date.now() - startTime;
      console.log(`[${TABLE_NAME}] Quick count complete: ${totalCount} items in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        sourceBreakdown, // ✅ NEW: Source breakdown included
        isEstimate: false,
        scannedInMs: elapsed,
        method: 'gsi-query-by-source',
        dateRange: { start: startDate, end: endDate }
      };
    } catch (error) {
      console.error('Error in quick count:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Count specific source in date range
   */
  static async _countSourceInDateRange(source, startDate, endDate) {
    let count = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#source = :source AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeNames: { '#source': 'source' },
        ExpressionAttributeValues: {
          ':source': source,
          ':start': startDate,
          ':end': endDate
        },
        Select: 'COUNT'
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      count += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return count;
  }

  /**
   * Fallback count without source breakdown
   */
  static async _countDateRangeWithoutSources(startDate, endDate) {
    const partitions = this.getMonthPartitions(startDate, endDate);
    const countPromises = partitions.map(partition => 
      this._countDatePartition(partition, startDate, endDate)
    );

    const results = await Promise.all(countPromises);
    const totalCount = results.reduce((sum, count) => sum + count, 0);

    return {
      totalLogs: totalCount,
      isEstimate: false,
      method: 'gsi-query',
      dateRange: { start: startDate, end: endDate }
    };
  }

  static async _countDatePartition(partition, startDate, endDate) {
    let count = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'createdAt-index',
        KeyConditionExpression: 'datePartition = :partition AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeValues: {
          ':partition': partition,
          ':start': startDate,
          ':end': endDate
        },
        Select: 'COUNT'
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      count += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return count;
  }

  /**
   * STREAMING STATISTICS (unchanged - already has source breakdown)
   */
  static async getStats(startDate, endDate, options = {}) {
    const startTime = Date.now();
    const { progressCallback } = options;

    if (!startDate || !endDate) {
      throw new Error('getStats() requires startDate and endDate parameters. For total count use getQuickStats().');
    }

    console.log(`[${TABLE_NAME}] Starting STREAMING stats for:`, startDate, 'to', endDate);

    try {
      const statsAggregator = this._createStatsAggregator();
      const partitions = this.getMonthPartitions(startDate, endDate);
      console.log(`[${TABLE_NAME}] Processing ${partitions.length} partitions...`);

      let totalProcessed = 0;
      for (let i = 0; i < partitions.length; i++) {
        const partition = partitions[i];
        console.log(`  Processing partition ${i + 1}/${partitions.length}: ${partition}`);
        
        const partitionCount = await this._streamProcessPartition(
          partition,
          startDate,
          endDate,
          statsAggregator,
          (count) => {
            totalProcessed += count;
            if (progressCallback) {
              progressCallback({
                partition: i + 1,
                totalPartitions: partitions.length,
                processedRecords: totalProcessed
              });
            }
          }
        );
        
        console.log(`    Processed ${partitionCount.toLocaleString()} records from ${partition}`);
      }

      const stats = this._finalizeStats(statsAggregator, startDate, endDate, startTime);
      
      const elapsed = Date.now() - startTime;
      console.log(`[${TABLE_NAME}] ✅ Streaming stats complete: ${stats.totalLogs.toLocaleString()} records in ${elapsed}ms`);
      
      return stats;
    } catch (error) {
      console.error('Error in getStats:', error);
      throw error;
    }
  }

  static _createStatsAggregator() {
    return {
      totalLogs: 0,
      seenPhones: new Set(),
      seenPans: new Set(),
      duplicatePhones: new Set(),
      duplicatePans: new Set(),
      sourceBreakdown: {},
      genderBreakdown: { 'Male': 0, 'Female': 0, 'Other': 0, 'Unknown': 0 },
      ageRangeBreakdown: {
        'Below 18': 0, '18-25': 0, '26-35': 0, '36-45': 0,
        '46-55': 0, '56-65': 0, 'Above 65': 0, 'Unknown': 0
      },
      jobTypeBreakdown: {},
      businessTypeBreakdown: {},
      salaryRangeBreakdown: {
        'Below 20k': 0, '20k-40k': 0, '40k-60k': 0,
        '60k-80k': 0, '80k-100k': 0, 'Above 100k': 0, 'Unknown': 0
      },
      creditScoreBreakdown: {
        'Poor (300-579)': 0, 'Fair (580-669)': 0, 'Good (670-739)': 0,
        'Very Good (740-799)': 0, 'Excellent (800-900)': 0, 'Unknown': 0
      },
      consentBreakdown: { 'true': 0, 'false': 0, 'unknown': 0 }
    };
  }

  static async _streamProcessPartition(partition, startDate, endDate, aggregator, progressCallback) {
    let lastKey = null;
    let partitionCount = 0;
    const CHUNK_SIZE = 1000;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'createdAt-index',
        KeyConditionExpression: 'datePartition = :partition AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeValues: {
          ':partition': partition,
          ':start': startDate,
          ':end': endDate
        },
        Limit: CHUNK_SIZE
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      const items = result.Items || [];
      
      this._processChunk(items, aggregator);
      
      partitionCount += items.length;
      lastKey = result.LastEvaluatedKey;
      
      if (progressCallback) {
        progressCallback(items.length);
      }
      
      if (lastKey) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    } while (lastKey);

    return partitionCount;
  }

  static _processChunk(items, aggregator) {
    items.forEach(item => {
      aggregator.totalLogs++;

      if (item.phone) {
        if (aggregator.seenPhones.has(item.phone)) {
          aggregator.duplicatePhones.add(item.phone);
        }
        aggregator.seenPhones.add(item.phone);
      }
      if (item.panNumber) {
        if (aggregator.seenPans.has(item.panNumber)) {
          aggregator.duplicatePans.add(item.panNumber);
        }
        aggregator.seenPans.add(item.panNumber);
      }

      const source = item.source || 'unknown';
      aggregator.sourceBreakdown[source] = (aggregator.sourceBreakdown[source] || 0) + 1;

      const gender = item.gender || 'Unknown';
      if (aggregator.genderBreakdown[gender] !== undefined) {
        aggregator.genderBreakdown[gender]++;
      } else {
        aggregator.genderBreakdown['Other']++;
      }

      let age = item.age;
      if (!age && item.dateOfBirth) {
        age = this.calculateAge(item.dateOfBirth);
      }
      const ageRange = age ? this.getAgeRange(age) : 'Unknown';
      aggregator.ageRangeBreakdown[ageRange]++;

      if (item.jobType) {
        aggregator.jobTypeBreakdown[item.jobType] = (aggregator.jobTypeBreakdown[item.jobType] || 0) + 1;
      }

      if (item.businessType) {
        aggregator.businessTypeBreakdown[item.businessType] = (aggregator.businessTypeBreakdown[item.businessType] || 0) + 1;
      }

      if (item.salary) {
        const salary = parseInt(item.salary);
        if (salary < 20000) aggregator.salaryRangeBreakdown['Below 20k']++;
        else if (salary < 40000) aggregator.salaryRangeBreakdown['20k-40k']++;
        else if (salary < 60000) aggregator.salaryRangeBreakdown['40k-60k']++;
        else if (salary < 80000) aggregator.salaryRangeBreakdown['60k-80k']++;
        else if (salary < 100000) aggregator.salaryRangeBreakdown['80k-100k']++;
        else aggregator.salaryRangeBreakdown['Above 100k']++;
      } else {
        aggregator.salaryRangeBreakdown['Unknown']++;
      }

      if (item.creditScore || item.cibilScore) {
        const score = item.creditScore || item.cibilScore;
        if (score < 580) aggregator.creditScoreBreakdown['Poor (300-579)']++;
        else if (score < 670) aggregator.creditScoreBreakdown['Fair (580-669)']++;
        else if (score < 740) aggregator.creditScoreBreakdown['Good (670-739)']++;
        else if (score < 800) aggregator.creditScoreBreakdown['Very Good (740-799)']++;
        else aggregator.creditScoreBreakdown['Excellent (800-900)']++;
      } else {
        aggregator.creditScoreBreakdown['Unknown']++;
      }

      const consent = item.consent === true ? 'true' : item.consent === false ? 'false' : 'unknown';
      aggregator.consentBreakdown[consent]++;
    });
  }

  static _finalizeStats(aggregator, startDate, endDate, startTime) {
    let uniqueLeads = 0;
    let duplicateLeads = 0;
    
    aggregator.seenPhones.forEach(phone => {
      if (aggregator.duplicatePhones.has(phone)) {
        duplicateLeads++;
      } else {
        uniqueLeads++;
      }
    });

    const elapsed = Date.now() - startTime;

    return {
      totalLogs: aggregator.totalLogs,
      uniqueLeads: uniqueLeads,
      duplicateLeads: duplicateLeads,
      duplicateByPhone: aggregator.duplicatePhones.size,
      duplicateByPan: aggregator.duplicatePans.size,
      dateRange: { start: startDate, end: endDate },
      sourceBreakdown: aggregator.sourceBreakdown,
      genderBreakdown: aggregator.genderBreakdown,
      ageRangeBreakdown: aggregator.ageRangeBreakdown,
      jobTypeBreakdown: aggregator.jobTypeBreakdown,
      businessTypeBreakdown: aggregator.businessTypeBreakdown,
      salaryRangeBreakdown: aggregator.salaryRangeBreakdown,
      creditScoreBreakdown: aggregator.creditScoreBreakdown,
      consentBreakdown: aggregator.consentBreakdown,
      processingTimeMs: elapsed,
      method: 'streaming-aggregation'
    };
  }

  static async getStatsByDate(startDate, endDate) {
    const startTime = Date.now();
    console.log(`[${TABLE_NAME}] Fetching stats by date (streaming):`, startDate, 'to', endDate);

    try {
      const statsByDate = {};
      const partitions = this.getMonthPartitions(startDate, endDate);

      for (const partition of partitions) {
        await this._streamProcessPartitionByDate(partition, startDate, endDate, statsByDate);
      }

      const result = Object.values(statsByDate).map(day => ({
        date: day.date,
        total: day.total,
        uniquePhones: day.uniquePhones.size,
        uniquePans: day.uniquePans.size,
        genders: day.genders,
        withConsent: day.withConsent,
        withoutConsent: day.withoutConsent
      })).sort((a, b) => a.date.localeCompare(b.date));

      const elapsed = Date.now() - startTime;
      console.log(`[${TABLE_NAME}] Stats by date complete in ${elapsed}ms`);

      return result;
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }

  static async _streamProcessPartitionByDate(partition, startDate, endDate, statsByDate) {
    let lastKey = null;
    const CHUNK_SIZE = 1000;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'createdAt-index',
        KeyConditionExpression: 'datePartition = :partition AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeValues: {
          ':partition': partition,
          ':start': startDate,
          ':end': endDate
        },
        Limit: CHUNK_SIZE
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      const items = result.Items || [];

      items.forEach(item => {
        const date = item.createdAt.split('T')[0];

        if (!statsByDate[date]) {
          statsByDate[date] = {
            date,
            total: 0,
            uniquePhones: new Set(),
            uniquePans: new Set(),
            genders: { Male: 0, Female: 0, Other: 0, Unknown: 0 },
            withConsent: 0,
            withoutConsent: 0
          };
        }

        statsByDate[date].total++;
        if (item.phone) statsByDate[date].uniquePhones.add(item.phone);
        if (item.panNumber) statsByDate[date].uniquePans.add(item.panNumber);

        const gender = item.gender || 'Unknown';
        if (statsByDate[date].genders[gender] !== undefined) {
          statsByDate[date].genders[gender]++;
        } else {
          statsByDate[date].genders['Other']++;
        }

        if (item.consent === true) {
          statsByDate[date].withConsent++;
        } else {
          statsByDate[date].withoutConsent++;
        }
      });

      lastKey = result.LastEvaluatedKey;
      
      if (lastKey) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    } while (lastKey);
  }

  // Migration functions (unchanged)
  static async backfillDatePartitions(sourceToMigrate = null) {
    let totalProcessed = 0;
    
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  MIGRATION: Backfill datePartition Field');
    console.log('═══════════════════════════════════════════════════════════\n');

    try {
      if (sourceToMigrate) {
        console.log(`[MIGRATION] Processing source: ${sourceToMigrate}`);
        const processed = await this._backfillSource(sourceToMigrate);
        totalProcessed += processed;
      } else {
        const sources = process.env.LEAD_SOURCES?.split(',') || [];
        
        if (sources.length === 0) {
          throw new Error('No sources configured. Set LEAD_SOURCES environment variable or provide source parameter.');
        }

        console.log(`[MIGRATION] Found ${sources.length} sources to process\n`);
        
        for (const source of sources) {
          const processed = await this._backfillSource(source.trim());
          totalProcessed += processed;
        }
      }

      console.log('\n═══════════════════════════════════════════════════════════');
      console.log('  ✅ MIGRATION COMPLETE!');
      console.log('═══════════════════════════════════════════════════════════');
      console.log(`  Total records updated: ${totalProcessed}`);
      console.log('═══════════════════════════════════════════════════════════\n');

      return { processed: totalProcessed };
    } catch (error) {
      console.error('\n❌ MIGRATION FAILED:', error.message);
      throw error;
    }
  }

  static async _backfillSource(source) {
    let processed = 0;
    let lastKey = null;

    console.log(`[MIGRATION] Processing source: ${source}`);

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#source = :source',
        ExpressionAttributeNames: { '#source': 'source' },
        ExpressionAttributeValues: { ':source': source },
        Limit: 100
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));

      const updates = (result.Items || [])
        .filter(item => !item.datePartition && item.createdAt)
        .map(item => {
          return this.updateByIdNoValidation(item.leadId, {
            datePartition: this.getDatePartition(item.createdAt)
          });
        });

      if (updates.length > 0) {
        await Promise.all(updates);
        processed += updates.length;
        console.log(`  ✓ Updated ${processed} records for ${source}...`);
      }
      
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    console.log(`  ✅ Completed ${source}: ${processed} records\n`);
    return processed;
  }
}

module.exports = Lead;