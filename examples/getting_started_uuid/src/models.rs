//! Models for the getting_started example.

use butane::prelude::*;
use butane::{model, FieldType, ForeignKey, Many, PrimaryKeyType};
use serde::{Deserialize, Serialize};

/// Blog identifier.
#[derive(Clone, Debug, Default, Deserialize, Serialize, FieldType, PartialEq, Eq)]
pub struct BlogId(pub uuid::Uuid);
impl PrimaryKeyType for BlogId {}

/// Blog metadata.
#[model]
#[derive(Debug, Default)]
pub struct Blog {
    /// Id of the blog.
    pub id: BlogId,
    /// Name of the blog.
    pub name: String,
}
impl Blog {
    /// Create a new Blog.
    pub fn new(name: impl Into<String>) -> Self {
        Blog {
            id: BlogId(uuid::Uuid::new_v4()),
            name: name.into(),
        }
    }
}

/// Post identifier.
#[derive(Clone, Debug, Default, Deserialize, Serialize, FieldType, PartialEq, Eq)]
pub struct PostId(pub uuid::Uuid);
impl PrimaryKeyType for PostId {}

/// Post details, including a [ForeignKey] to [Blog]
/// and a [Many] relationship to [Tag]s.
#[model]
pub struct Post {
    /// Id of the blog post.
    pub id: PostId,
    /// Title of the blog post.
    pub title: String,
    /// Body of the blog post.
    pub body: String,
    /// Whether the blog post has been published.
    pub published: bool,
    /// Tags for the blog post.
    pub tags: Many<Tag>,
    /// The [Blog] this post is attached to.
    pub blog: ForeignKey<Blog>,
    /// Byline of the post.
    pub byline: Option<String>,
    /// How many likes this post has.
    pub likes: i32,
}
impl Post {
    /// Create a new Post.
    pub fn new(blog: &Blog, title: String, body: String) -> Self {
        Post {
            id: PostId(uuid::Uuid::new_v4()),
            title,
            body,
            published: false,
            tags: Many::default(),
            blog: blog.into(),
            byline: None,
            likes: 0,
        }
    }
}

/// Tags to be associated with a [Post].
#[model]
#[derive(Debug, Default)]
pub struct Tag {
    /// Tag name.
    #[pk]
    pub tag: String,
}
impl Tag {
    /// Create a new Tag.
    pub fn new(tag: impl Into<String>) -> Self {
        Tag { tag: tag.into() }
    }
}
