use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};
use std::path::Path;
use std::fs;

pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    schema: Schema,
}

impl SearchIndex {
    pub fn new(path: &str, fields: &[String]) -> anyhow::Result<Self> {
        let mut schema_builder = Schema::builder();
        
        // Add row_id as a stored string field
        schema_builder.add_text_field("row_id", STRING | STORED);
        
        // Add indexed text fields
        for field in fields {
            schema_builder.add_text_field(field, TEXT | STORED);
        }
        
        let schema = schema_builder.build();
        
        if !Path::new(path).exists() {
            fs::create_dir_all(path)?;
        }
        
        let index = if let Ok(index) = Index::open_in_dir(path) {
            index
        } else {
            Index::create_in_dir(path, schema.clone())?
        };

        let writer = index.writer(50_000_000)?; // 50MB heap
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            index,
            reader,
            writer,
            schema,
        })
    }

    pub fn add_document(&mut self, row_id: &str, field_values: &[(String, String)]) -> anyhow::Result<()> {
        let mut doc = TantivyDocument::default();
        let row_id_field = self.schema.get_field("row_id").unwrap();
        doc.add_text(row_id_field, row_id);
        
        for (field_name, value) in field_values {
            if let Ok(field) = self.schema.get_field(field_name) {
                doc.add_text(field, value);
            }
        }
        
        self.writer.add_document(doc)?;
        self.writer.commit()?;
        Ok(())
    }

    pub fn delete_document(&mut self, row_id: &str) -> anyhow::Result<()> {
        let row_id_field = self.schema.get_field("row_id").unwrap();
        let term = tantivy::Term::from_field_text(row_id_field, row_id);
        self.writer.delete_term(term);
        self.writer.commit()?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize) -> anyhow::Result<Vec<(String, f32)>> {
        let searcher = self.reader.searcher();
        let fields: Vec<Field> = self.schema.fields()
            .filter(|(f, _)| self.schema.get_field_name(*f) != "row_id")
            .map(|(f, _)| f)
            .collect();
            
        let query_parser = QueryParser::for_index(&self.index, fields);
        let query = query_parser.parse_query(query_str)?;
        
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        
        let mut results = Vec::new();
        let row_id_field = self.schema.get_field("row_id").unwrap();
        
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(val) = retrieved_doc.get_first(row_id_field) {
                if let Some(id_str) = val.as_str() {
                    results.push((id_str.to_string(), score));
                }
            }
        }
        
        Ok(results)
    }
}
