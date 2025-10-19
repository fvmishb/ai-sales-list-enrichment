# 🚨 Current Issues & Technical Debt

## 🔥 Critical Issues

### Issue #1: Address Quality Regression
**Priority**: 🔴 Critical  
**Status**: Open  
**Description**: 
- GENERIC addresses increased from 1,677 to 3,961 companies
- Good addresses decreased from 454 to 291 companies
- Processing logic is causing data quality degradation instead of improvement

**Root Cause**: 
- Processing targets wrong data source (raw vs enriched table)
- New companies being processed instead of existing GENERIC addresses

**Expected Behavior**: 
- GENERIC addresses should decrease
- Good addresses should increase
- Processing should target enriched table with existing GENERIC addresses

**Files Affected**:
- `src/handlers/simple_processor.py`
- `src/main.py` (process-generic-addresses endpoint)

---

### Issue #2: API Authentication Failures
**Priority**: 🟡 High  
**Status**: Partially Resolved  
**Description**: 
- Persistent Gemini API 401 errors
- Authentication method inconsistency between direct API key and Vertex AI

**Current Status**: 
- Switched to Vertex AI authentication
- Still experiencing intermittent 401 errors

**Files Affected**:
- `src/services/simple_gemini_client.py`
- `src/config.py`

---

### Issue #3: Processing Logic Confusion
**Priority**: 🟡 High  
**Status**: Open  
**Description**: 
- Multiple processing endpoints with overlapping functionality
- Unclear data flow between raw and enriched tables
- Batch processing targets wrong data source

**Endpoints Affected**:
- `/fast-process` (processes raw table)
- `/process-generic-addresses` (should process enriched table)

---

## 🟠 Medium Priority Issues

### Issue #4: Rate Limiting Problems
**Priority**: 🟠 Medium  
**Status**: Partially Resolved  
**Description**: 
- Google Custom Search API 429 errors
- Aggressive parallel processing hitting rate limits

**Current Mitigation**: 
- Reduced batch sizes and parallel workers
- Added delays between batches

---

### Issue #5: Data Schema Inconsistencies
**Priority**: 🟠 Medium  
**Status**: Resolved  
**Description**: 
- BigQuery schema mismatch for pain_hypotheses field
- Fixed: Changed from STRING to ARRAY<STRING>

---

### Issue #6: Duplicate Processing
**Priority**: 🟠 Medium  
**Status**: Resolved  
**Description**: 
- Multiple background processes running simultaneously
- Resource waste and potential data conflicts

**Resolution**: 
- Implemented process management
- Added process cleanup

---

## 🟢 Low Priority Issues

### Issue #7: Monitoring & Observability
**Priority**: 🟢 Low  
**Status**: Open  
**Description**: 
- Limited real-time processing visibility
- No comprehensive dashboard for data quality metrics
- Manual log checking required

**Proposed Solution**: 
- Implement real-time monitoring dashboard
- Add data quality metrics API
- Create automated alerts

---

### Issue #8: Error Handling
**Priority**: 🟢 Low  
**Status**: Open  
**Description**: 
- Generic error messages
- Limited error context for debugging
- No error categorization

---

## 🔧 Technical Debt

### Issue #9: Code Organization
**Priority**: 🟢 Low  
**Status**: Open  
**Description**: 
- Multiple similar processing scripts
- Inconsistent error handling patterns
- Mixed responsibilities in some modules

**Files to Refactor**:
- `scripts/` directory (multiple similar scripts)
- `src/services/` (overlapping functionality)

---

### Issue #10: Documentation
**Priority**: 🟢 Low  
**Status**: Open  
**Description**: 
- API documentation missing
- Code comments inconsistent
- No troubleshooting guide for common issues

---

## 📋 Action Items

### Immediate (Next 24 hours)
1. **Fix Issue #1**: Correct processing logic to target enriched table
2. **Fix Issue #3**: Clarify data flow and endpoint responsibilities
3. **Test Issue #2**: Verify Gemini API authentication stability

### Short Term (Next Week)
1. **Address Issue #4**: Implement proper rate limiting strategy
2. **Address Issue #7**: Add basic monitoring capabilities
3. **Address Issue #8**: Improve error handling and logging

### Long Term (Next Month)
1. **Address Issue #9**: Refactor code organization
2. **Address Issue #10**: Complete documentation
3. **Performance Optimization**: Review and optimize processing pipeline

---

## 🎯 Success Metrics

### Data Quality Targets
- **GENERIC addresses**: < 500 companies (currently 3,961)
- **Good addresses**: > 3,500 companies (currently 291)
- **Processing success rate**: > 95%

### Performance Targets
- **Processing speed**: 20-30 companies/minute
- **API error rate**: < 5%
- **Data accuracy**: > 90% for critical fields

---

## 📞 Next Steps

1. **Create GitHub repository** ✅
2. **Create individual GitHub issues** for each item above
3. **Assign priorities** and **assignees**
4. **Set up project board** for tracking
5. **Implement fixes** starting with Critical issues

---

*Last Updated: 2025-10-18*
*Total Issues: 10 (3 Critical, 2 High, 2 Medium, 3 Low)*
