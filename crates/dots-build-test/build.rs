fn main() {
    dots_rs_build::compile(&["proto/types.dots"]).expect("dots-build compile failed");
}
