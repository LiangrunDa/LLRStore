use std::{env, fs, path::Path};

fn main() {
    let current_dir = env::current_dir().unwrap();
    let proto_path = Path::new(&current_dir).join("protos");
    let mut proto_files = vec![];
    for entry in fs::read_dir(&proto_path).unwrap() {
        let entry = entry.unwrap();
        let md = entry.metadata().unwrap();
        if md.is_file() && entry.path().extension().unwrap() == "proto" {
            proto_files.push(entry.path().as_os_str().to_os_string())
        }
    }
    println!("proto_files: {:?}", proto_files);

    tonic_build::configure()
        .out_dir("src") // generate files in /protos_generated
        .build_client(true) // generate client side code
        .build_server(true) // generate server side code
        .compile(
            proto_files.as_slice(), // proto files
            &[&proto_path],         // include path
        )
        .unwrap();
}
