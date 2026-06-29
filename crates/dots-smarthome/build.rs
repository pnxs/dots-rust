fn main() {
    dots_rs_build::compile(&["proto/model.dots"]).expect("dots-build compile failed");
}
