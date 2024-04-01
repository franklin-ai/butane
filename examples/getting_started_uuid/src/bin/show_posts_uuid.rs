use butane::query;
use getting_started_uuid::models::Post;
use getting_started_uuid::*;

fn main() {
    let conn = establish_connection();
    let results = query!(Post, published == true)
        .limit(5)
        .load(&conn)
        .expect("Error loading posts");
    println!("Displaying {} posts", results.len());
    for post in results {
        println!("{} ({} likes)", post.title, post.likes);
        println!("----------\n");
        println!("{}", post.body);
    }
}
