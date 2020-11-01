(module
  (import "main" "memory" (memory $mem 1))
  (func $x (export "x")
    (local $z v128)

    (local.set $z (v128.load (i32.const 0)))

    (v128.store (i32.const 0) (v128.const i32x4 0x11223344 0x55667788 0x99aabbcc 0xddeeff00))

    ;; (v128.store align=16 (i32.const 16)
    ;;   (i8x16.add (v128.const i8x16 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1) (v128.const i8x16 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1)))
  )
)
