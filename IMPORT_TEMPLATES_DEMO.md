# Example: How Default Templates Appear to Users

## 📚 Import Books Interface

When users navigate to the "Import Books" page, they will now see:

### Supported Formats Section
```
📝 Supported Formats:
• Goodreads Export: Standard Goodreads library export CSV [Default Template Available]
• StoryGraph Export: StoryGraph library export CSV [Default Template Available]  
• Simple ISBN List: Single column with ISBNs/UPCs
• Custom CSV: Any CSV with book data (Title, Author, ISBN, etc.)
```

## 🗺️ Field Mapping Interface

When users upload a CSV file, they'll see:

### Template Selection Dropdown
```
🔧 Import Template (Optional)
┌─────────────────────────────────────────────────┐
│ -- No Template (Map Manually) --               │
│ Goodreads Export (Default) ✓ (Default)         │
│ StoryGraph Export (Default) ✓ (Default)        │
│ My Custom Template (Custom)                     │
└─────────────────────────────────────────────────┘

Import templates save your field mappings for reuse with similar CSV files.
Default templates are provided for common formats like Goodreads and StoryGraph.
```

### Automatic Detection
When uploading a Goodreads CSV, the system will:
1. Analyze headers like "Title", "Author", "My Rating", "Exclusive Shelf"
2. Calculate 85%+ match with Goodreads template  
3. Automatically pre-select "Goodreads Export (Default)" template
4. Show message: "✅ Automatically detected: Goodreads Export format"

### Pre-mapped Fields
With Goodreads template selected, fields are automatically mapped:
```
CSV Column → Book Field
─────────────────────────
Title → Title ✓
Author → Author ✓  
My Rating → My Rating ✓
Exclusive Shelf → Reading Status ✓
Date Read → Date Read ✓
Bookshelves → Categories ✓
My Review → Notes/Review ✓
```

## 📋 Template Management Page

In the Templates management interface:

```
Your Saved Templates

Name                           Source   Fields   Usage   Last Used   Actions
────────────────────────────────────────────────────────────────────────────
Goodreads Export (Default)    Goodreads   16      0      Never      [View] [System]
StoryGraph Export (Default)   StoryGraph  13      0      Never      [View] [System]  
My Custom Mapping             Custom      8       3      2024-01-15 [View] [Delete]
```

System templates show:
- 🔵 "System Default" badge
- Cannot be deleted (button shows "System" and is disabled)
- Available to all users automatically

## 🎯 Benefits for Users

### For New Users
- Import from Goodreads/StoryGraph with zero configuration
- No need to learn field mapping on first use
- Immediate success importing their existing data

### For Experienced Users  
- Templates serve as starting points for customization
- Can create variations of default templates
- Faster imports for standard formats

### Rich Metadata Preservation
StoryGraph template creates custom fields for:
- 🎭 **Moods**: "dark, hopeful, adventurous, emotional"
- ⚡ **Pace**: "fast", "medium", "slow" 
- 👥 **Character vs Plot**: Character/plot driven analysis
- ⚠️ **Content Warnings**: Sensitive topic tags

This preserves the unique metadata that makes StoryGraph special while making it available in Bibliotheca.

## 🔄 Template Auto-Update

Templates are created automatically when the app starts, so:
- New installations get templates immediately
- Updates can add new templates or improve existing ones
- No manual setup required for administrators
- Templates are always available and current

This creates a much better import experience that rivals commercial book tracking services!
