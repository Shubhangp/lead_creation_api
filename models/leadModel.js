// models/leadModel.js
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

const TABLE_NAME = 'leads';

class Lead {
  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  // Get date partition for GSI (format: "YYYY-MM")
  static getDatePartition(date) {
    const d = new Date(date);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  }

  // Get all month partitions between two dates
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

  // Calculate age from date of birth
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

  // Get age range category
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

    // Required fields
    if (!data.source) errors.push('Source is required');
    if (!data.fullName) errors.push('Full name is required');
    if (!data.phone) errors.push('Phone is required');
    if (!data.email) errors.push('Email is required');
    if (!data.panNumber) errors.push('PAN number is required');

    // String length validations
    if (data.fullName && (data.fullName.length < 1 || data.fullName.length > 100)) {
      errors.push('Full name must be between 1 and 100 characters');
    }
    if (data.firstName && (data.firstName.length < 1 || data.firstName.length > 50)) {
      errors.push('First name must be between 1 and 50 characters');
    }
    if (data.lastName && (data.lastName.length < 1 || data.lastName.length > 50)) {
      errors.push('Last name must be between 1 and 50 characters');
    }

    // Email validation
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}$/;
    if (data.email && !emailRegex.test(data.email)) {
      errors.push('Invalid email format');
    }

    // PAN number validation
    const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
    if (data.panNumber && !panRegex.test(data.panNumber)) {
      errors.push('Invalid PAN number format (e.g., ABCDE1234F)');
    }

    // Age validation
    if (data.age !== undefined && (data.age < 18 || data.age > 120)) {
      errors.push('Age must be between 18 and 120');
    }

    // Date of birth validation
    if (data.dateOfBirth) {
      const dob = new Date(data.dateOfBirth);
      if (dob > new Date()) {
        errors.push('Date of birth cannot be in the future');
      }
    }

    // Credit score validation
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

  // Create lead with uniqueness check
  static async create(leadData) {
    // Validate data
    this.validate(leadData);

    // Check if phone already exists
    const existingPhone = await this.findByPhone(leadData.phone);
    if (existingPhone) {
      const error = new Error('Phone number already exists');
      error.code = 'DUPLICATE_PHONE';
      throw error;
    }

    // Check if PAN already exists
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
      datePartition: this.getDatePartition(now) // For createdAt-index GSI
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(leadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));

    return result.Item || null;
  }

  // Find by phone (GSI)
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

  // Find by PAN number (GSI)
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

  // Find by source with date sorting
  static async findBySource(source, options = {}) {
    const {
      limit = 100,
      startDate,
      endDate,
      sortAscending = false,
      lastEvaluatedKey = null
    } = options;

    // Use ExpressionAttributeNames to handle 'source' reserved keyword
    let keyConditionExpression = '#source = :source';
    const expressionAttributeNames = {
      '#source': 'source'
    };
    const expressionAttributeValues = { ':source': source };

    // Add date range if provided
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

    // Add pagination token if provided
    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    // Return both items and pagination token
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey || null,
      count: result.Count || 0
    };
  }

  // Find all leads (paginated) - DEPRECATED: Use findByDateRange instead
  // This method is kept for backward compatibility but should be avoided
  static async findAll(options = {}) {
    console.warn('[DEPRECATED] findAll() uses expensive Scan operation. Use findByDateRange() instead.');
    
    const { limit = 100, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new ScanCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  // Find by date range using GSI (OPTIMIZED - No Scan)
  static async findByDateRange(startDate, endDate, options = {}) {
    const { limit = 1000, lastEvaluatedKey } = options;
    
    try {
      // Get all month partitions in the date range
      const partitions = this.getMonthPartitions(startDate, endDate);
      
      // Query each partition
      const promises = partitions.map(partition => 
        this._queryByDatePartition(partition, startDate, endDate, limit)
      );
      
      const results = await Promise.all(promises);
      const allItems = results.flat();
      
      // Apply limit if specified
      const items = limit ? allItems.slice(0, limit) : allItems;
      
      return {
        items,
        lastEvaluatedKey: null, // Note: Pagination across partitions needs custom implementation
        count: items.length
      };
    } catch (error) {
      console.error('Error in findByDateRange:', error);
      throw error;
    }
  }

  // Helper: Query single date partition
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
      
      // Break if we've hit the limit
      if (limit && items.length >= limit) {
        break;
      }
    } while (lastKey);

    return items;
  }

  // Update lead
  static async updateById(leadId, updates) {
    // Validate updates if they contain validatable fields
    if (Object.keys(updates).length > 0) {
      // Get existing lead for validation
      const existingLead = await this.findById(leadId);
      if (!existingLead) {
        throw new Error('Lead not found');
      }

      // Merge and validate
      const mergedData = { ...existingLead, ...updates };
      this.validate(mergedData);

      // Check uniqueness if phone or PAN is being updated
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

  // Delete lead
  static async deleteById(leadId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));

    return { deleted: true };
  }

  // Query by multiple filters - DEPRECATED: Use specific GSI queries instead
  // This method uses expensive Scan and should be avoided
  static async findByFilters(filters = {}, options = {}) {
    console.warn('[DEPRECATED] findByFilters() uses expensive Scan operation. Use specific GSI queries instead.');
    
    const { limit = 100 } = options;

    const filterExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(filters).forEach((key, index) => {
      filterExpressions.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = filters[key];
    });

    const params = {
      TableName: TABLE_NAME,
      Limit: limit
    };

    if (filterExpressions.length > 0) {
      params.FilterExpression = filterExpressions.join(' AND ');
      params.ExpressionAttributeNames = expressionAttributeNames;
      params.ExpressionAttributeValues = expressionAttributeValues;
    }

    const result = await docClient.send(new ScanCommand(params));

    return result.Items || [];
  }

  // Count leads by source
  static async countBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: {
        '#source': 'source'
      },
      ExpressionAttributeValues: { ':source': source },
      Select: 'COUNT'
    }));

    return result.Count || 0;
  }

  // ============================================================================
  // STATISTICS FUNCTIONS (OPTIMIZED)
  // ============================================================================

  // Get quick stats with optional date range (COUNT only - very fast)
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    try {
      if (source && !startDate) {
        // Get count for specific source using GSI
        const count = await this.countBySource(source);
        return {
          totalLogs: count,
          source: source,
          isEstimate: false
        };
      } else if (startDate && endDate) {
        // Quick count for date range using GSI
        return this.getQuickStatsForDateRange(startDate, endDate);
      } else {
        // Use parallel scan for total count (fallback - expensive)
        console.warn('Full table count requested - this is expensive!');
        return this.getQuickStatsParallel();
      }
    } catch (error) {
      console.error('Error in getQuickStats:', error);
      throw error;
    }
  }

  // Quick count for date range using GSI (OPTIMIZED)
  static async getQuickStatsForDateRange(startDate, endDate) {
    const startTime = Date.now();

    try {
      console.log(`[${TABLE_NAME}] Quick count for date range:`, startDate, 'to', endDate);

      // Get all month partitions
      const partitions = this.getMonthPartitions(startDate, endDate);
      
      // Count each partition in parallel
      const countPromises = partitions.map(partition => 
        this._countDatePartition(partition, startDate, endDate)
      );

      const results = await Promise.all(countPromises);
      const totalCount = results.reduce((sum, count) => sum + count, 0);
      const elapsed = Date.now() - startTime;

      console.log(`[${TABLE_NAME}] Quick count complete: ${totalCount} items in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        isEstimate: false,
        scannedInMs: elapsed,
        method: 'gsi-query',
        dateRange: { start: startDate, end: endDate }
      };
    } catch (error) {
      console.error('Error in quick count:', error);
      throw error;
    }
  }

  // Helper: Count items in date partition
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

    console.log(`Count partition ${partition}: ${count} items`);
    return count;
  }

  // Parallel scan for total count (DISABLED - Too expensive)
  static async getQuickStatsParallel() {
    throw new Error('Full table scans are disabled for cost optimization. Please provide a date range to getQuickStats(source, startDate, endDate).');
  }

  // Get comprehensive stats (OPTIMIZED for date ranges using GSI)
  static async getStats(startDate = null, endDate = null) {
    const startTime = Date.now();

    // REQUIRE date range to prevent expensive full table scans
    if (!startDate || !endDate) {
      throw new Error('getStats() requires startDate and endDate parameters. Full table scans are disabled for cost optimization. Use getQuickStats() for simple counts or provide a date range.');
    }

    console.log(`[${TABLE_NAME}] Starting stats fetch for date range:`, startDate, 'to', endDate);

    try {
      // Use GSI to query by date range (OPTIMIZED)
      const partitions = this.getMonthPartitions(startDate, endDate);
      
      const promises = partitions.map(partition => 
        this._queryByDatePartition(partition, startDate, endDate)
      );
      
      const results = await Promise.all(promises);
      const allItems = results.flat();
      
      console.log(`[${TABLE_NAME}] GSI query complete: ${allItems.length} items in ${Date.now() - startTime}ms`);

      // Process stats data
      return this.processStatsData(allItems, startDate, endDate, startTime);
      
    } catch (error) {
      console.error('Error in getStats:', error);
      throw error;
    }
  }

  // Process stats data (extracted for reusability)
  static processStatsData(allItems, startDate, endDate, startTime) {
    // Track unique phones and PANs for duplicate detection
    const seenPhones = new Set();
    const seenPans = new Set();
    const duplicatePhones = new Set();
    const duplicatePans = new Set();

    // First pass - identify duplicates
    allItems.forEach(item => {
      if (item.phone) {
        if (seenPhones.has(item.phone)) {
          duplicatePhones.add(item.phone);
        }
        seenPhones.add(item.phone);
      }
      if (item.panNumber) {
        if (seenPans.has(item.panNumber)) {
          duplicatePans.add(item.panNumber);
        }
        seenPans.add(item.panNumber);
      }
    });

    // Initialize stats
    const stats = {
      totalLogs: allItems.length,
      uniqueLeads: 0,
      duplicateLeads: 0,
      duplicateByPhone: duplicatePhones.size,
      duplicateByPan: duplicatePans.size,
      dateRange: {
        start: startDate,
        end: endDate
      },
      sourceBreakdown: {},
      genderBreakdown: {
        'Male': 0,
        'Female': 0,
        'Other': 0,
        'Unknown': 0
      },
      ageRangeBreakdown: {
        'Below 18': 0,
        '18-25': 0,
        '26-35': 0,
        '36-45': 0,
        '46-55': 0,
        '56-65': 0,
        'Above 65': 0,
        'Unknown': 0
      },
      jobTypeBreakdown: {},
      businessTypeBreakdown: {},
      salaryRangeBreakdown: {
        'Below 20k': 0,
        '20k-40k': 0,
        '40k-60k': 0,
        '60k-80k': 0,
        '80k-100k': 0,
        'Above 100k': 0,
        'Unknown': 0
      },
      creditScoreBreakdown: {
        'Poor (300-579)': 0,
        'Fair (580-669)': 0,
        'Good (670-739)': 0,
        'Very Good (740-799)': 0,
        'Excellent (800-900)': 0,
        'Unknown': 0
      },
      consentBreakdown: {
        'true': 0,
        'false': 0,
        'unknown': 0
      }
    };

    // Second pass - process stats
    allItems.forEach(item => {
      // Check if lead is duplicate
      const isDuplicate = duplicatePhones.has(item.phone) || duplicatePans.has(item.panNumber);
      if (isDuplicate) {
        stats.duplicateLeads++;
      } else {
        stats.uniqueLeads++;
      }

      // Source breakdown
      const source = item.source || 'unknown';
      stats.sourceBreakdown[source] = (stats.sourceBreakdown[source] || 0) + 1;

      // Gender breakdown
      const gender = item.gender || 'Unknown';
      if (stats.genderBreakdown[gender] !== undefined) {
        stats.genderBreakdown[gender]++;
      } else {
        stats.genderBreakdown['Other']++;
      }

      // Age range breakdown (calculated from DOB)
      let age = item.age;
      if (!age && item.dateOfBirth) {
        age = this.calculateAge(item.dateOfBirth);
      }
      const ageRange = age ? this.getAgeRange(age) : 'Unknown';
      stats.ageRangeBreakdown[ageRange]++;

      // Job type breakdown
      if (item.jobType) {
        stats.jobTypeBreakdown[item.jobType] = (stats.jobTypeBreakdown[item.jobType] || 0) + 1;
      }

      // Business type breakdown
      if (item.businessType) {
        stats.businessTypeBreakdown[item.businessType] = (stats.businessTypeBreakdown[item.businessType] || 0) + 1;
      }

      // Salary range breakdown
      if (item.salary) {
        const salary = parseInt(item.salary);
        if (salary < 20000) {
          stats.salaryRangeBreakdown['Below 20k']++;
        } else if (salary < 40000) {
          stats.salaryRangeBreakdown['20k-40k']++;
        } else if (salary < 60000) {
          stats.salaryRangeBreakdown['40k-60k']++;
        } else if (salary < 80000) {
          stats.salaryRangeBreakdown['60k-80k']++;
        } else if (salary < 100000) {
          stats.salaryRangeBreakdown['80k-100k']++;
        } else {
          stats.salaryRangeBreakdown['Above 100k']++;
        }
      } else {
        stats.salaryRangeBreakdown['Unknown']++;
      }

      // Credit score breakdown
      if (item.creditScore || item.cibilScore) {
        const score = item.creditScore || item.cibilScore;
        if (score < 580) {
          stats.creditScoreBreakdown['Poor (300-579)']++;
        } else if (score < 670) {
          stats.creditScoreBreakdown['Fair (580-669)']++;
        } else if (score < 740) {
          stats.creditScoreBreakdown['Good (670-739)']++;
        } else if (score < 800) {
          stats.creditScoreBreakdown['Very Good (740-799)']++;
        } else {
          stats.creditScoreBreakdown['Excellent (800-900)']++;
        }
      } else {
        stats.creditScoreBreakdown['Unknown']++;
      }

      // Consent breakdown
      const consent = item.consent === true ? 'true' : item.consent === false ? 'false' : 'unknown';
      stats.consentBreakdown[consent]++;
    });

    const elapsed = Date.now() - startTime;
    console.log(`[${TABLE_NAME}] Stats processing complete in ${elapsed}ms`);
    stats.processingTimeMs = elapsed;

    return stats;
  }

  // Get stats grouped by date (OPTIMIZED using GSI)
  static async getStatsByDate(startDate, endDate) {
    const startTime = Date.now();
    console.log(`[${TABLE_NAME}] Fetching stats by date:`, startDate, 'to', endDate);

    try {
      // Use GSI to query by date range
      const partitions = this.getMonthPartitions(startDate, endDate);
      
      const promises = partitions.map(partition => 
        this._queryByDatePartition(partition, startDate, endDate)
      );
      
      const results = await Promise.all(promises);
      const allItems = results.flat();

      console.log(`[${TABLE_NAME}] Fetched ${allItems.length} items in ${Date.now() - startTime}ms`);

      // Group by date
      const statsByDate = {};

      allItems.forEach(item => {
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

        if (item.phone) {
          statsByDate[date].uniquePhones.add(item.phone);
        }
        if (item.panNumber) {
          statsByDate[date].uniquePans.add(item.panNumber);
        }

        // Gender tracking
        const gender = item.gender || 'Unknown';
        if (statsByDate[date].genders[gender] !== undefined) {
          statsByDate[date].genders[gender]++;
        } else {
          statsByDate[date].genders['Other']++;
        }

        // Consent tracking
        if (item.consent === true) {
          statsByDate[date].withConsent++;
        } else {
          statsByDate[date].withoutConsent++;
        }
      });

      // Convert sets to counts
      const result = Object.values(statsByDate).map(day => ({
        date: day.date,
        total: day.total,
        uniquePhones: day.uniquePhones.size,
        uniquePans: day.uniquePans.size,
        genders: day.genders,
        withConsent: day.withConsent,
        withoutConsent: day.withoutConsent
      })).sort((a, b) => a.date.localeCompare(b.date));

      return result;
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }

  // ============================================================================
  // MIGRATION / UTILITY FUNCTIONS
  // ============================================================================

  // Backfill datePartition for existing records (Run once after adding createdAt-index GSI)
  // Uses source-createdAt-index to avoid full table scan
  static async backfillDatePartitions(source = null) {
    let processed = 0;
    
    console.log('[MIGRATION] Starting datePartition backfill...');

    if (source) {
      // Backfill specific source using GSI
      let lastKey = null;
      
      do {
        const params = {
          TableName: TABLE_NAME,
          IndexName: 'source-createdAt-index',
          KeyConditionExpression: '#source = :source',
          ExpressionAttributeNames: {
            '#source': 'source'
          },
          ExpressionAttributeValues: {
            ':source': source
          },
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
          console.log(`[MIGRATION] Backfilled ${processed} items for source: ${source}...`);
        }
        
        lastKey = result.LastEvaluatedKey;
      } while (lastKey);
    } else {
      // Get all unique sources first
      console.log('[MIGRATION] Getting all sources...');
      const sources = await this.getAllSources();
      
      console.log(`[MIGRATION] Found ${sources.length} sources. Processing each...`);
      
      // Process each source
      for (const sourceValue of sources) {
        await this.backfillDatePartitions(sourceValue);
      }
    }

    console.log(`[MIGRATION] Backfill complete: ${processed} items updated`);
    return { processed };
  }

  // Get all unique sources (helper for backfill)
  static async getAllSources() {
    const sources = new Set();
    let lastKey = null;

    // We need one scan here to get unique sources, but it's SELECT specific attributes only
    do {
      const params = {
        TableName: TABLE_NAME,
        ProjectionExpression: '#source',
        ExpressionAttributeNames: {
          '#source': 'source'
        },
        Limit: 1000
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      
      (result.Items || []).forEach(item => {
        if (item.source) {
          sources.add(item.source);
        }
      });
      
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return Array.from(sources);
  }
}

module.exports = Lead;