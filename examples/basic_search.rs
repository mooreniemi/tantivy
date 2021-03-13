// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// - create an index in a directory
// - index a few documents into our index
// - search for the best document matching a basic query
// - retrieve the best document's original content.

// ---
// Importing tantivy...
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, ReloadPolicy};
use std::fs;
// use tempfile::TempDir;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    //let index_path = TempDir::new()?;

    let index_path = "/tmp/tantivy";
    fs::create_dir_all(index_path)?;

    // # Defining the schema
    //
    // The Tantivy index requires a very strict schema.
    // The schema declares which fields are in the index,
    // and for each field, its type and "the way it should
    // be indexed".

    // First we need to define a schema ...
    let mut schema_builder = Schema::builder();

    // Our first field is title.
    // We want full-text search for it, and we also want
    // to be able to retrieve the document after the search.
    //
    // `TEXT | STORED` is some syntactic sugar to describe
    // that.
    //
    // `TEXT` means the field should be tokenized and indexed,
    // along with its term frequency and term positions.
    //
    // `STORED` means that the field will also be saved
    // in a compressed, row-oriented key-value store.
    // This store is useful for reconstructing the
    // documents that were selected during the search phase.
    // We can make our index lighter by omitting the `STORED` flag.
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("url", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT | STORED);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Let's create a brand new index.
    //
    // This will actually just save a meta.json
    // with our schema in the directory.
    let directory = MmapDirectory::open(&index_path)?;
    let index = Index::open_or_create(directory, schema.clone())?;

    // To insert a document we will need an index writer.
    // There must be only one writer at a time.
    // This single `IndexWriter` is already
    // multithreaded.
    //
    // Here we give tantivy a budget of `50MB`.
    // Using a bigger heap for the indexer may increase
    // throughput, but 50 MB is already plenty.
    let mut index_writer = index.writer(50_000_000)?;

    // Let's index our documents!
    // We first need a handle on the title and the body field.

    // ### Adding documents
    //
    // We can create a document manually, by setting the fields
    // one by one in a Document object.
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
    let url = schema.get_field("url").unwrap();

    let paths = vec![
        "/home/alex/git/parquet/1000-wiki.parquet",
    ];

    let file = fs::File::open(&std::path::Path::new(paths[0])).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(row) = iter.next() {
        let r_url = row.get_string(0).unwrap();
        let r_title = row.get_string(1).unwrap();
        let r_body = row.get_string(2).unwrap();
        // println!("{}", r_url);

        // For convenience, tantivy also comes with a macro to
        // reduce the boilerplate above.
        index_writer.add_document(doc!(
                url => format!("{}", r_url),
                title => format!("{}", r_title),
                body => format!("{}", r_body))
        );
    }

    // This is an example, so we will only index 3 documents
    // here. You can check out tantivy's tutorial to index
    // the English wikipedia. Tantivy's indexing is rather fast.
    // Indexing 5 million articles of the English wikipedia takes
    // around 3 minutes on my computer!

    // ### Committing
    //
    // At this point our documents are not searchable.
    //
    //
    // We need to call `.commit()` explicitly to force the
    // `index_writer` to finish processing the documents in the queue,
    // flush the current index to the disk, and advertise
    // the existence of new documents.
    //
    // This call is blocking.
    index_writer.commit()?;

    // If `.commit()` returns correctly, then all of the
    // documents that have been added are guaranteed to be
    // persistently indexed.
    //
    // In the scenario of a crash or a power failure,
    // tantivy behaves as if it has rolled back to its last
    // commit.

    // # Searching
    //
    // ### Searcher
    //
    // A reader is required first in order to search an index.
    // It acts as a `Searcher` pool that reloads itself,
    // depending on a `ReloadPolicy`.
    //
    // For a search server you will typically create one reader for the entire lifetime of your
    // program, and acquire a new searcher for every single request.
    //
    // In the code below, we rely on the 'ON_COMMIT' policy: the reader
    // will reload the index automatically after each commit.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    // We now need to acquire a searcher.
    //
    // A searcher points to a snapshotted, immutable version of the index.
    //
    // Some search experience might require more than
    // one query. Using the same searcher ensures that all of these queries will run on the
    // same version of the index.
    //
    // Acquiring a `searcher` is very cheap.
    //
    // You should acquire a searcher every time you start processing a request and
    // and release it right after your query is finished.
    let searcher = reader.searcher();

    // ### Query

    // The query parser can interpret human queries.
    // Here, if the user does not specify which
    // field they want to search, tantivy will search
    // in both title and body.
    let query_parser = QueryParser::for_index(&index, vec![title, body]);

    // `QueryParser` may fail if the query is not in the right
    // format. For user facing applications, this can be a problem.
    // A ticket has been opened regarding this problem.
    let query = query_parser.parse_query("indiana")?;

    // A query defines a set of documents, as
    // well as the way they should be scored.
    //
    // A query created by the query parser is scored according
    // to a metric called Tf-Idf, and will consider
    // any document matching at least one of our terms.

    // ### Collectors
    //
    // We are not interested in all of the documents but
    // only in the top 10. Keeping track of our top 10 best documents
    // is the role of the `TopDocs` collector.

    // We can now perform our query.
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    // The actual documents still need to be
    // retrieved from Tantivy's store.
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}
