"""
Conference corpus management with Couchbase integration
"""
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.vector_search import VectorQuery, VectorSearch
from couchbase.search import SearchRequest, MatchNoneQuery
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from dotenv import load_dotenv

from ..config.openrouter_client import OpenRouterClient

load_dotenv()

class CollectionNotReadyError(Exception):
    """Raised when Couchbase collection is not ready for operations"""
    pass

class ConferenceCorpusManager:
    def __init__(self):
        self.openrouter_client = OpenRouterClient()
        self._initialize_couchbase()
    
    def _initialize_couchbase(self):
        """Initialize Couchbase connection"""
        try:
            connection_string = os.getenv('CB_CONNECTION_STRING')
            username = os.getenv('CB_USERNAME')
            password = os.getenv('CB_PASSWORD')
            bucket_name = os.getenv('CB_BUCKET', 'conferences')
            
            if not all([connection_string, username, password]):
                raise ValueError("Missing required Couchbase environment variables")
            
            auth = PasswordAuthenticator(username, password)
            timeout_options = ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=30),  # Increased timeout
                query_timeout=timedelta(seconds=30),
                search_timeout=timedelta(seconds=30)
            )
            options = ClusterOptions(auth, timeout_options=timeout_options)
            
            self.cluster = Cluster(connection_string, options)
            self.cluster.ping()
            
            self.bucket = self.cluster.bucket(bucket_name)
            self.scope = self.bucket.scope("_default")
            
            print(f"✅ Connected to Couchbase bucket: {bucket_name}")
            
        except Exception as e:
            print(f"❌ Failed to initialize Couchbase: {str(e)}")
            raise

    async def _ensure_collection_exists(self, collection_name: str):
        """Ensure collection exists and is fully ready for operations"""
        import asyncio
        
        max_wait_time = 60  # Maximum wait time in seconds
        start_time = datetime.utcnow()
        
        try:
            collection_manager = self.bucket.collections()
            
            # Step 1: Check if collection exists, create if not
            print(f"🔧 Checking collection: {collection_name}")
            collection_exists = False
            
            try:
                collections = collection_manager.get_all_scopes()
                for scope in collections:
                    if scope.name == "_default":
                        for collection in scope.collections:
                            if collection.name == collection_name:
                                collection_exists = True
                                print(f"✅ Collection already exists: {collection_name}")
                                break
                        break
                
                if not collection_exists:
                    print(f"🔧 Creating collection: {collection_name}")
                    # Use new API to avoid deprecation warning
                    collection_manager.create_collection(
                        scope_name="_default",
                        collection_name=collection_name
                    )
                    print(f"✅ Collection created: {collection_name}")
                    # Give initial time for creation
                    await asyncio.sleep(5)
                
            except Exception as create_error:
                print(f"⚠️ Collection creation warning: {str(create_error)}")
                # Continue with validation even if creation had warnings
            
            # Step 2: Get collection reference
            collection = self.bucket.collection(collection_name)
            
            # Step 3: Comprehensive readiness validation
            print(f"🧪 Validating collection readiness...")
            
            validation_attempts = 0
            max_validation_attempts = 12  # 12 attempts * 5s = 60s max
            
            while validation_attempts < max_validation_attempts:
                try:
                    elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                    print(f"⏳ Collection validation attempt {validation_attempts + 1}/{max_validation_attempts} ({elapsed_time:.0f}s elapsed)")
                    
                    # Test 1: Basic upsert operation
                    test_key = f"_readiness_test_{datetime.utcnow().timestamp()}"
                    test_doc = {
                        "test": True,
                        "timestamp": datetime.utcnow().isoformat(),
                        "validation_attempt": validation_attempts + 1
                    }
                    
                    collection.upsert(test_key, test_doc, timeout=timedelta(seconds=15))
                    
                    # Test 2: Retrieve operation
                    result = collection.get(test_key, timeout=timedelta(seconds=10))
                    if not result or not result.value:
                        raise Exception("Document retrieval failed")
                    
                    # Test 3: Update operation
                    test_doc["updated"] = True
                    collection.upsert(test_key, test_doc, timeout=timedelta(seconds=10))
                    
                    # Test 4: Batch operation test (3 documents)
                    batch_keys = []
                    for i in range(3):
                        batch_key = f"_batch_test_{i}_{datetime.utcnow().timestamp()}"
                        collection.upsert(batch_key, {"batch_test": i}, timeout=timedelta(seconds=10))
                        batch_keys.append(batch_key)
                    
                    # Clean up test documents
                    collection.remove(test_key)
                    for batch_key in batch_keys:
                        collection.remove(batch_key)
                    
                    print(f"✅ Collection {collection_name} is fully operational and ready!")
                    return collection
                    
                except Exception as validation_error:
                    validation_attempts += 1
                    elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                    
                    if elapsed_time >= max_wait_time:
                        error_msg = f"Collection validation timeout after {max_wait_time}s. Last error: {str(validation_error)}"
                        print(f"❌ {error_msg}")
                        raise CollectionNotReadyError(error_msg)
                    
                    if validation_attempts < max_validation_attempts:
                        wait_time = 5  # Fixed 5-second intervals
                        print(f"⚠️ Validation failed: {str(validation_error)}")
                        print(f"⏳ Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        error_msg = f"Collection failed validation after {validation_attempts} attempts. Final error: {str(validation_error)}"
                        print(f"❌ {error_msg}")
                        raise CollectionNotReadyError(error_msg)
            
            # This should not be reached, but added for safety
            raise CollectionNotReadyError(f"Collection validation failed after maximum attempts")
            
        except CollectionNotReadyError:
            # Re-raise our custom exception
            raise
        except Exception as e:
            error_msg = f"Unexpected error ensuring collection exists: {str(e)}"
            print(f"❌ {error_msg}")
            raise CollectionNotReadyError(error_msg)
    
    async def store_conference_corpus(
        self, 
        conference_info: Dict[str, Any], 
        talks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Store conference corpus in Couchbase with embeddings"""
        
        conference_id = conference_info['id']
        collection_name = f"talks_{conference_id}"
        
        try:
            # Create and ensure collection is ready
            collection = await self._ensure_collection_exists(collection_name)
            
            print(f"📚 Storing {len(talks)} talks for conference: {conference_id}")
            
            # Store conference metadata
            metadata_doc = {
                **conference_info,
                'stored_at': datetime.utcnow().isoformat(),
                'total_talks': len(talks),
                'embedding_model': self.openrouter_client.embedding_model
            }
            
            try:
                collection.upsert(f"metadata_{conference_id}", metadata_doc)
                print(f"✅ Stored conference metadata")
            except Exception as e:
                print(f"⚠️ Warning storing metadata: {str(e)}")
            
            # Store talks with embeddings using batch processing
            successful_stores = 0
            failed_stores = 0
            batch_size = 10
            batch_delay = 2  # seconds between batches
            
            print(f"📦 Processing {len(talks)} talks in batches of {batch_size}")
            
            # Process talks in batches
            import asyncio
            for batch_start in range(0, len(talks), batch_size):
                batch_end = min(batch_start + batch_size, len(talks))
                batch = talks[batch_start:batch_end]
                batch_num = (batch_start // batch_size) + 1
                total_batches = (len(talks) + batch_size - 1) // batch_size
                
                print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} talks)...")
                
                batch_success = 0
                batch_failures = 0
                
                for i, talk in enumerate(batch):
                    global_index = batch_start + i
                    try:
                        # Generate document key
                        doc_key = self._generate_talk_key(talk, global_index)
                        
                        # Generate embedding
                        combined_text = self._create_embedding_text(talk)
                        embedding = self.openrouter_client.generate_embedding(combined_text)
                        
                        # Add embedding to talk data
                        talk_doc = {
                            **talk,
                            'conference_id': conference_id,
                            'embedding': embedding,
                            'embedding_text': combined_text,
                            'stored_at': datetime.utcnow().isoformat(),
                            'batch_number': batch_num
                        }
                        
                        # Store in Couchbase with extended timeout
                        collection.upsert(doc_key, talk_doc, timeout=timedelta(seconds=20))
                        batch_success += 1
                        successful_stores += 1
                        
                    except Exception as e:
                        print(f"❌ Error storing talk {global_index} (batch {batch_num}): {str(e)}")
                        batch_failures += 1
                        failed_stores += 1
                        continue
                
                print(f"✅ Batch {batch_num} complete: {batch_success} success, {batch_failures} failures")
                
                # Add delay between batches (except for the last batch)
                if batch_end < len(talks):
                    print(f"⏳ Waiting {batch_delay}s before next batch...")
                    await asyncio.sleep(batch_delay)
            
            print(f"🎉 Batch processing complete!")
            print(f"✅ Successfully stored {successful_stores} talks")
            if failed_stores > 0:
                print(f"⚠️ Failed to store {failed_stores} talks")
            
            # Create search index if needed
            print(f"🔍 Now creating search index for collection: {collection_name}")
            try:
                await self._ensure_search_index(collection_name)
                print(f"✅ Search index creation process completed for: {collection_name}")
            except Exception as index_error:
                print(f"❌ Search index creation failed for {collection_name}: {str(index_error)}")
                print(f"💡 You can create the index manually using: python create_search_indexes.py create {conference_id}")
            
            return {
                'conference_id': conference_id,
                'collection_name': collection_name,
                'successful_stores': successful_stores,
                'failed_stores': failed_stores,
                'total_talks': len(talks)
            }
            
        except Exception as e:
            print(f"❌ Error storing conference corpus: {str(e)}")
            raise
    
    def _generate_talk_key(self, talk: Dict[str, Any], index: int) -> str:
        """Generate unique document key for talk"""
        
        # Try to extract ID from URL
        url = talk.get('url', '')
        if url:
            # Extract last part of URL as ID
            url_parts = url.rstrip('/').split('/')
            if len(url_parts) > 0:
                url_id = url_parts[-1]
                if url_id and url_id != 'event':  # Avoid generic terms
                    return f"talk_{url_id}"
        
        # Fallback: use index and title hash
        title = talk.get('title', 'unknown')
        title_hash = abs(hash(title)) % 10000
        return f"talk_{index}_{title_hash}"
    
    def _create_embedding_text(self, talk: Dict[str, Any]) -> str:
        """Create text for embedding generation"""
        
        parts = []
        
        # Add title with weight
        title = talk.get('title', '').strip()
        if title and title != 'Unknown Title':
            parts.append(f"Title: {title}")
        
        # Add description
        description = talk.get('description', '').strip()
        if description and description not in ['No description available', '']:
            parts.append(f"Description: {description}")
        
        # Add category
        category = talk.get('category', '').strip()
        if category and category not in ['Uncategorized', '']:
            parts.append(f"Category: {category}")
        
        # Add speaker
        speaker = talk.get('speaker', '').strip()
        if speaker and speaker not in ['Unknown Speaker', 'Unknown']:
            parts.append(f"Speaker: {speaker}")
        
        return '\n'.join(parts) if parts else title or 'Unknown talk'
    
    def _get_embedding_dimensions(self) -> int:
        """Detect embedding dimensions for the current model"""
        model_dimensions = {
            "BAAI/bge-large-en-v1.5": 1024,
            "BAAI/bge-base-en-v1.5": 768,
            "sentence-transformers/all-MiniLM-L6-v2": 384,
            "intfloat/e5-mistral-7b-instruct": 4096,
            "text-embedding-ada-002": 1536,
        }
        
        # Get dimensions for current model
        dimensions = model_dimensions.get(self.openrouter_client.embedding_model, 1024)
        print(f"🔧 Using {dimensions} dimensions for embedding model: {self.openrouter_client.embedding_model}")
        return dimensions

    async def _ensure_search_index(self, collection_name: str):
        """Ensure vector search index exists for collection"""
        import asyncio
        
        index_name = f"vector_search_{collection_name}"
        print(f"🔧 Starting index creation process for: {index_name}")
        
        try:
            # Get search index manager
            search_index_manager = self.cluster.search_indexes()
            print("✅ Connected to search index manager")
            
            # Check if index already exists
            try:
                existing_indexes = search_index_manager.get_all_indexes()
                existing_names = [idx.name for idx in existing_indexes]
                
                if index_name in existing_names:
                    print(f"✅ Vector search index already exists: {index_name}")
                    return
                else:
                    print(f"🔧 Index not found in existing indexes: {existing_names}")
                    print(f"🔧 Creating new index: {index_name}")
                    
            except Exception as list_error:
                print(f"⚠️ Could not list existing indexes: {str(list_error)}")
                print(f"🔧 Proceeding with index creation: {index_name}")
            
            # Get embedding dimensions for current model
            embedding_dims = self._get_embedding_dimensions()
            
            # Index definition following official Couchbase documentation format
            index_definition = {
                "type": "fulltext-index",
                "name": index_name,
                "sourceType": "couchbase",
                "sourceName": self.bucket.name,
                "planParams": {
                    "maxPartitionsPerPIndex": 512,
                    "indexPartitions": 1
                },
                "params": {
                    "doc_config": {
                        "docid_prefix_delim": "",
                        "docid_regexp": "",
                        "mode": "scope.collection.type_field",
                        "type_field": "type"
                    },
                    "mapping": {
                        "analysis": {},
                        "default_analyzer": "standard",
                        "default_datetime_parser": "dateTimeOptional",
                        "default_field": "_all",
                        "default_mapping": {
                            "dynamic": False,
                            "enabled": False
                        },
                        "default_type": "_default",
                        "docvalues_dynamic": False,
                        "index_dynamic": False,
                        "store_dynamic": False,
                        "type_field": "_type",
                        "types": {
                            f"_default.{collection_name}": {
                                "dynamic": False,
                                "enabled": True,
                                "properties": {
                                    "embedding": {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [
                                            {
                                                "dims": embedding_dims,
                                                "index": True,
                                                "name": "embedding",
                                                "similarity": "dot_product",
                                                "type": "vector"
                                            }
                                        ]
                                    },
                                    "title": {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [
                                            {
                                                "analyzer": "standard",
                                                "index": True,
                                                "name": "title",
                                                "store": True,
                                                "type": "text"
                                            }
                                        ]
                                    },
                                    "description": {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [
                                            {
                                                "analyzer": "standard",
                                                "index": True,
                                                "name": "description",
                                                "store": True,
                                                "type": "text"
                                            }
                                        ]
                                    },
                                    "category": {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [
                                            {
                                                "analyzer": "keyword",
                                                "index": True,
                                                "name": "category",
                                                "store": True,
                                                "type": "text"
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    "store": {
                        "indexType": "scorch",
                        "segmentVersion": 16
                    }
                },
                "sourceParams": {}
            }
            
            # Create the index
            print(f"🔧 Creating vector search index with definition...")
            print(f"📊 Embedding dimensions: {embedding_dims}")
            
            search_index_manager.upsert_index(index_definition)
            print(f"✅ Index creation request submitted: {index_name}")
            
            # Wait for index to be ready with simplified checking
            max_wait_time = 60  # 1 minute max wait (reduced)
            start_time = datetime.utcnow()
            
            print("⏳ Waiting for index to be ready...")
            
            for attempt in range(6):  # 6 attempts, 10 seconds each
                try:
                    await asyncio.sleep(10)  # Wait 10 seconds between checks
                    
                    # Try to get the index to see if it exists
                    index_info = search_index_manager.get_index(index_name)
                    print(f"✅ Index found and operational: {index_name}")
                    break
                    
                except Exception as check_error:
                    elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                    print(f"⏳ Index building... attempt {attempt + 1}/6 ({elapsed_time:.0f}s elapsed)")
                    
                    if elapsed_time >= max_wait_time:
                        print(f"⚠️ Index creation timeout after {max_wait_time}s")
                        print(f"💡 Index may still be building in background")
                        print(f"💡 Check Couchbase Web Console for status")
                        break
            
            print(f"🎉 Vector search index setup complete: {index_name}")
            
        except Exception as e:
            print(f"❌ Error creating search index {index_name}: {str(e)}")
            print(f"📋 Error details: {type(e).__name__}")
            print(f"💡 Index name: {index_name}")
            print(f"💡 Bucket: {self.bucket.name}")
            print(f"💡 Collection: {collection_name}")
            print(f"💡 Embedding dimensions: {self._get_embedding_dimensions()}")
            # Don't raise exception - allow system to continue without search functionality
    
    def get_similar_talks(
        self, 
        query: str, 
        conference_id: str, 
        num_results: int = 5
    ) -> List[Dict[str, Any]]:
        """Find similar talks using vector search or fallback to text search"""
        
        # First try vector search
        vector_results = self._try_vector_search(query, conference_id, num_results)
        if vector_results:
            return vector_results
        
        # Fallback to text search if vector search fails
        print("🔄 Vector search unavailable, falling back to text search...")
        return self._fallback_text_search(query, conference_id, num_results)
    
    def _try_vector_search(self, query: str, conference_id: str, num_results: int) -> List[Dict[str, Any]]:
        """Try vector search (requires Couchbase 7.1+ with vector search enabled)"""
        
        try:
            collection_name = f"talks_{conference_id}"
            collection = self.bucket.collection(collection_name)
            index_name = f"vector_search_{collection_name}"
            
            # Generate query embedding
            query_embedding = self.openrouter_client.generate_embedding(query)
            
            # Create vector search request
            search_req = SearchRequest.create(MatchNoneQuery()).with_vector_search(
                VectorSearch.from_vector_query(
                    VectorQuery("embedding", query_embedding, num_candidates=num_results)
                )
            )
            
            # Execute search
            result = self.scope.search(index_name, search_req, timeout=timedelta(seconds=20))
            rows = list(result.rows())
            
            similar_talks = []
            for row in rows:
                try:
                    doc = collection.get(row.id, timeout=timedelta(seconds=5))
                    if doc and doc.value:
                        talk = doc.value
                        similar_talks.append({
                            "title": talk.get("title", "N/A"),
                            "description": talk.get("description", "N/A"),
                            "category": talk.get("category", "N/A"),
                            "speaker": talk.get("speaker", "N/A"),
                            "score": row.score,
                            "url": talk.get("url", ""),
                            "conference_id": talk.get("conference_id", "")
                        })
                except Exception as doc_error:
                    print(f"⚠️ Could not fetch document {row.id}: {doc_error}")
                    continue
            
            return similar_talks
            
        except Exception as e:
            print(f"⚠️ Vector search failed (this is normal for older Couchbase versions): {str(e)}")
            return []
    
    def _fallback_text_search(self, query: str, conference_id: str, num_results: int) -> List[Dict[str, Any]]:
        """Fallback text-based similarity search using SQL++ queries"""
        
        try:
            collection_name = f"talks_{conference_id}"
            
            # Extract keywords from query
            query_words = self._extract_keywords(query)
            if not query_words:
                return []
            
            print(f"🔍 Searching for keywords: {query_words}")
            
            # Build SQL++ query for text similarity
            where_conditions = []
            
            # Search in title and description
            for word in query_words:
                word_lower = word.lower()
                where_conditions.append(f"(LOWER(title) LIKE '%{word_lower}%' OR LOWER(description) LIKE '%{word_lower}%' OR LOWER(category) LIKE '%{word_lower}%')")
            
            where_clause = " OR ".join(where_conditions)
            
            # Query with scoring based on keyword matches
            sql_query = f"""
            SELECT 
                title, 
                description, 
                category, 
                speaker, 
                url, 
                conference_id,
                META().id,
                (
                    {' + '.join([f"(CASE WHEN LOWER(title) LIKE '%{word.lower()}%' THEN 3 ELSE 0 END)" for word in query_words])} +
                    {' + '.join([f"(CASE WHEN LOWER(description) LIKE '%{word.lower()}%' THEN 2 ELSE 0 END)" for word in query_words])} +
                    {' + '.join([f"(CASE WHEN LOWER(category) LIKE '%{word.lower()}%' THEN 1 ELSE 0 END)" for word in query_words])}
                ) as relevance_score
            FROM `{self.bucket.name}`.`_default`.`{collection_name}`
            WHERE ({where_clause}) AND META().id NOT LIKE 'metadata_%'
            ORDER BY relevance_score DESC
            LIMIT {num_results * 2}
            """
            
            print(f"🔍 Executing text search query...")
            result = self.cluster.query(sql_query, timeout=timedelta(seconds=30))
            rows = list(result)
            
            # Process results
            similar_talks = []
            for row in rows[:num_results]:
                if row.get('relevance_score', 0) > 0:  # Only include relevant results
                    similar_talks.append({
                        "title": row.get("title", "N/A"),
                        "description": row.get("description", "N/A"),
                        "category": row.get("category", "N/A"),
                        "speaker": row.get("speaker", "N/A"),
                        "score": row.get("relevance_score", 0) / 10.0,  # Normalize score
                        "url": row.get("url", ""),
                        "conference_id": row.get("conference_id", "")
                    })
            
            print(f"✅ Found {len(similar_talks)} similar talks using text search")
            return similar_talks
            
        except Exception as e:
            print(f"❌ Error during text search: {str(e)}")
            
            # Final fallback: return random sample
            return self._get_random_talks(conference_id, num_results)
    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract meaningful keywords from query"""
        
        # Simple keyword extraction (can be enhanced with NLP)
        import re
        
        # Remove common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'from', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do',
            'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'this', 'that',
            'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them'
        }
        
        # Extract words (3+ characters)
        words = re.findall(r'\b\w{3,}\b', query.lower())
        keywords = [word for word in words if word not in stop_words]
        
        return keywords[:5]  # Limit to 5 keywords
    
    def _get_random_talks(self, conference_id: str, num_results: int) -> List[Dict[str, Any]]:
        """Get random talks as final fallback"""
        
        try:
            collection_name = f"talks_{conference_id}"
            
            sql_query = f"""
            SELECT title, description, category, speaker, url, conference_id
            FROM `{self.bucket.name}`.`_default`.`{collection_name}`
            WHERE META().id NOT LIKE 'metadata_%'
            LIMIT {num_results}
            """
            
            result = self.cluster.query(sql_query, timeout=timedelta(seconds=10))
            rows = list(result)
            
            similar_talks = []
            for i, row in enumerate(rows):
                similar_talks.append({
                    "title": row.get("title", "N/A"),
                    "description": row.get("description", "N/A"),
                    "category": row.get("category", "N/A"),
                    "speaker": row.get("speaker", "N/A"),
                    "score": 0.5,  # Default score
                    "url": row.get("url", ""),
                    "conference_id": row.get("conference_id", "")
                })
            
            print(f"✅ Returning {len(similar_talks)} random talks as fallback")
            return similar_talks
            
        except Exception as e:
            print(f"❌ Error getting random talks: {str(e)}")
            return []
    
    def list_conferences(self) -> List[Dict[str, Any]]:
        """List all stored conferences"""
        
        try:
            conferences = []
            
            # Get all collections in the default scope
            collection_manager = self.bucket.collections()
            scopes = collection_manager.get_all_scopes()
            
            for scope in scopes:
                if scope.name == "_default":
                    for collection_info in scope.collections:
                        collection_name = collection_info.name
                        
                        # Skip system collections and default collection
                        if collection_name in ['_default'] or collection_name.startswith('_'):
                            continue
                        
                        # Look for talks collections
                        if collection_name.startswith('talks_'):
                            try:
                                # Try to get metadata from this collection
                                collection = self.bucket.collection(collection_name)
                                
                                # Extract conference ID from collection name
                                conference_id = collection_name.replace('talks_', '')
                                metadata_key = f"metadata_{conference_id}"
                                
                                # Try to get the metadata document
                                try:
                                    metadata_doc = collection.get(metadata_key, timeout=timedelta(seconds=10))
                                    if metadata_doc and metadata_doc.value:
                                        conf_data = metadata_doc.value
                                        conferences.append({
                                            'id': conf_data.get('id', conference_id),
                                            'name': conf_data.get('name', 'Unknown Conference'),
                                            'year': conf_data.get('year', 'Unknown'),
                                            'platform': conf_data.get('platform', 'Unknown'),
                                            'total_talks': conf_data.get('total_talks', 0),
                                            'stored_at': conf_data.get('stored_at', ''),
                                            'collection_name': collection_name,
                                            'embedding_model': conf_data.get('embedding_model', 'Unknown')
                                        })
                                        print(f"✅ Found conference: {conf_data.get('name', conference_id)}")
                                        
                                except DocumentNotFoundException:
                                    print(f"⚠️ No metadata found for collection: {collection_name}")
                                    
                                    # Try to get basic info from collection documents
                                    try:
                                        # Query a few documents to extract basic info
                                        query = f"SELECT title, conference_id, stored_at FROM `{self.bucket.name}`.`_default`.`{collection_name}` WHERE META().id NOT LIKE 'metadata_%' LIMIT 5"
                                        result = self.cluster.query(query)
                                        rows = list(result)
                                        
                                        if rows:
                                            # Create basic conference info from available data
                                            first_doc = rows[0]
                                            conferences.append({
                                                'id': conference_id,
                                                'name': f"Conference {conference_id}",
                                                'year': 'Unknown',
                                                'platform': 'Unknown',
                                                'total_talks': len(rows),
                                                'stored_at': first_doc.get('stored_at', ''),
                                                'collection_name': collection_name,
                                                'embedding_model': 'Unknown'
                                            })
                                            print(f"✅ Reconstructed conference info for: {collection_name}")
                                        
                                    except Exception as query_error:
                                        print(f"⚠️ Could not query collection {collection_name}: {str(query_error)}")
                                        
                            except Exception as collection_error:
                                print(f"⚠️ Error accessing collection {collection_name}: {str(collection_error)}")
                                continue
            
            print(f"📊 Found {len(conferences)} conferences total")
            return sorted(conferences, key=lambda x: x.get('stored_at', ''), reverse=True)
            
        except Exception as e:
            print(f"❌ Error listing conferences: {str(e)}")
            return []
    
    def get_conference_stats(self, conference_id: str) -> Dict[str, Any]:
        """Get detailed statistics for a conference"""
        
        try:
            collection_name = f"talks_{conference_id}"
            
            # Get metadata
            metadata_doc = self.bucket.collection(collection_name).get(f"metadata_{conference_id}")
            metadata = metadata_doc.value if metadata_doc else {}
            
            # Get talk count
            query = f"SELECT COUNT(*) as count FROM `{self.bucket.name}`.`_default`.`{collection_name}` WHERE META().id NOT LIKE 'metadata_%'"
            result = self.cluster.query(query)
            talk_count = list(result)[0]['count']
            
            # Get category distribution
            category_query = f"SELECT category, COUNT(*) as count FROM `{self.bucket.name}`.`_default`.`{collection_name}` WHERE META().id NOT LIKE 'metadata_%' GROUP BY category ORDER BY count DESC"
            category_result = self.cluster.query(category_query)
            categories = [{'category': row['category'], 'count': row['count']} for row in category_result]
            
            return {
                'conference_id': conference_id,
                'metadata': metadata,
                'total_talks': talk_count,
                'category_distribution': categories
            }
            
        except Exception as e:
            print(f"❌ Error getting conference stats: {str(e)}")
            return {'error': str(e)}
    
    def delete_conference(self, conference_id: str) -> bool:
        """Delete a conference and all its talks"""
        
        try:
            collection_name = f"talks_{conference_id}"
            
            # Delete all documents in the collection
            query = f"DELETE FROM `{self.bucket.name}`.`_default`.`{collection_name}`"
            self.cluster.query(query)
            
            print(f"✅ Deleted conference: {conference_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error deleting conference: {str(e)}")
            return False
    
    def close(self):
        """Close Couchbase connection"""
        try:
            self.cluster.close()
        except Exception as e:
            print(f"⚠️ Error closing Couchbase connection: {str(e)}")

# Convenience functions for easy integration
async def store_crawled_conference(crawl_result: Dict[str, Any]) -> Dict[str, Any]:
    """Store results from parallel crawler"""
    
    corpus_manager = ConferenceCorpusManager()
    
    try:
        if not crawl_result['success']:
            raise ValueError(f"Crawl failed: {crawl_result.get('error')}")
        
        return await corpus_manager.store_conference_corpus(
            crawl_result['conference'],
            crawl_result['talks']
        )
        
    finally:
        corpus_manager.close()

def search_conference_talks(conference_id: str, query: str, num_results: int = 5) -> List[Dict[str, Any]]:
    """Quick search function"""
    
    corpus_manager = ConferenceCorpusManager()
    
    try:
        return corpus_manager.get_similar_talks(query, conference_id, num_results)
    finally:
        corpus_manager.close()
