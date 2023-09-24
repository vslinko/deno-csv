(module    
  (import "js" "mem" (memory $mem 0))
  (import "js" "log" (func $log (param i32)))

  (global $col_sep_size (export "col_sep_size") i32 (i32.const 4096))
  (global $line_sep_size (export "line_sep_size") i32 (i32.const 4096))
  (global $qoute_size (export "qoute_size") i32 (i32.const 4096))
  (global $input_size (export "input_size") i32 (i32.const 65536))
  (global $cell_size (export "cell_size") i32 (i32.const 65536))

  (global $col_sep_len_idx (export "col_sep_len_idx") i32 (i32.const 0))    ;; 0
  (global $line_sep_len_idx (export "line_sep_len_idx") i32 (i32.const 4))   ;; 0+4
  (global $qoute_len_idx (export "qoute_len_idx") i32 (i32.const 8))      ;; 4+4
  (global $input_len_idx (export "input_len_idx") i32 (i32.const 12))     ;; 8+4
  (global $cell_len_idx (export "cell_len_idx") i32 (i32.const 16))     ;; 12+4

  (global $col_sep_idx (export "col_sep_idx") i32 (i32.const 128))      ;; 32*4
  (global $line_sep_idx (export "line_sep_idx") i32 (i32.const 4224))    ;; 128+4096
  (global $qoute_idx (export "qoute_idx") i32 (i32.const 8320))       ;; 4224+4096
  (global $input_idx (export "input_idx") i32 (i32.const 65536))      ;; 65536
  (global $cell_idx (export "cell_idx") i32 (i32.const 131072))      ;; 65536*2

  (global $state_begin i32 (i32.const 0))
  (global $state_need_more_data i32 (i32.const 1))
  (global $state_eof i32 (i32.const 2))
  (global $state_error i32 (i32.const 3))
  (global $state_cell i32 (i32.const 14))
  (global $state_cell_and_newline i32 (i32.const 15))
  (global $state_cell_and_eof i32 (i32.const 16))

  (global $state (mut i32) (i32.const 0))
  (global $input_read_idx (mut i32) (i32.const 0))
  (global $cell_write_idx (mut i32) (i32.const 0))
  (global $input_eof (mut i32) (i32.const 0))
  (global $min_possible_buffer_reserve (mut i32) (i32.const 0))

  (func $get_input_read_idx (export "getInputReadIndex") (result i32) (global.get $input_read_idx))
  (func $get_cell_write_idx (export "getCellWriteIdx") (result i32) (global.get $cell_write_idx))

  (func $set_eof (export "setEof") (global.set $input_eof (i32.const 1)))

  ;; (func $col_sep_len (result i32) (i32.load (global.get $col_sep_len_idx)))
  ;; (func $line_sep_len (result i32) (i32.load (global.get $line_sep_len_idx)))
  ;; (func $input_len (result i32) (i32.load (global.get $input_len_idx)))
  ;; (func $input_unprocessed (result i32)
  ;;   (i32.sub (call $input_len) (global.get $input_read_idx)))

  (func $max (param $a i32) (param $b i32) (result i32)
    (if (result i32)
      (i32.gt_u (local.get $a) (local.get $b))
        (local.get $a)
        (local.get $b)))

  (func $min (param $a i32) (param $b i32) (result i32)
    (if (result i32)
      (i32.gt_u (local.get $a) (local.get $b))
        (local.get $b)
        (local.get $a)))

  (func $memcmp (param $a i32) (param $a_len i32) (param $b i32) (param $b_len i32) (result i32)
    (local $i i32)
    (local $max i32)
    (local $res i32)

    (local.set $i (i32.const 0))
    (local.set $res (i32.const 0))
    (local.set $max (call $min (local.get $a_len) (local.get $b_len)))

    (block $_block
      (loop $_loop
        (if (i32.ge_u (local.get $i) (local.get $max))
          (block
            (local.set $res (i32.const 1))
            (br $_block)))

        (if
          (i32.ne
            (i32.load8_u (i32.add (local.get $a) (local.get $i)))
            (i32.load8_u (i32.add (local.get $b) (local.get $i))))
          (block
            (br $_block)))

        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $_loop))

      (unreachable))
      
    (local.get $res))

  ;; (func $is_line_sep (result i32)
  ;;   (call $memcmp
  ;;     (global.get $line_sep_idx) (call $line_sep_len)
  ;;     (i32.add (global.get $input_idx) (global.get $input_read_idx)) (call $input_unprocessed)))

  ;; (func $is_col_sep (result i32)
  ;;   (call $memcmp
  ;;     (global.get $col_sep_idx) (call $col_sep_len)
  ;;     (i32.add (global.get $input_idx) (global.get $input_read_idx)) (call $input_unprocessed)))

  (func $setup (export "setup")
    (global.set $min_possible_buffer_reserve
      (call $max (i32.load (global.get $col_sep_len_idx)) (i32.load (global.get $line_sep_len_idx)))))

  ;; (func $need_more_memory (result i32)
  ;;   (i32.ge_u
  ;;     (i32.add (global.get $cell_write_idx) (global.get $min_possible_buffer_reserve))
  ;;     (i32.mul (i32.sub (memory.size) (i32.const 2)) (i32.const 65536))))

  (func $reset_input_idx (export "resetInputIndex")
    (global.set $input_read_idx (i32.const 0)))

  (func $read (export "read") (result i32)
    (local $i i32)
    (local $a i32)
    (local $b i32)
    (local $max i32)
    (local $res i32)
    (local $_input_unprocessed i32)

    (block $_block
      ;; если уже все прочли, то больше нечего делать
      (if (i32.eq (global.get $state) (global.get $state_eof))
        (br $_block))

      ;; если уже была ошибка, то больше нечего делать
      (if (i32.eq (global.get $state) (global.get $state_error))
        (br $_block))

      ;; если в прошлый раз была последняя колонка, то завершаем работу
      (if (i32.eq (global.get $state) (global.get $state_cell_and_eof))
        (block
          (global.set $state (global.get $state_eof))
          (br $_block)))

      ;; если в прошлый раз была последняя колонка в строке, то пропускаем разделитель линии и очищаем буффер
      (if (i32.eq (global.get $state) (global.get $state_cell_and_newline))
        (block
          (global.set $input_read_idx (i32.add (global.get $input_read_idx) (i32.load (global.get $line_sep_len_idx))))
          (global.set $cell_write_idx (i32.const 0))
          (global.set $state (i32.const 0))))

      ;; если в прошлый раз была колонка, то пропускаем разделитель колонки и очищаем буффер
      (if (i32.eq (global.get $state) (global.get $state_cell))
        (block
          (global.set $input_read_idx (i32.add (global.get $input_read_idx) (i32.load (global.get $col_sep_len_idx))))
          (global.set $cell_write_idx (i32.const 0))
          (global.set $state (i32.const 0))))

      (loop $_loop
        (local.set $_input_unprocessed (i32.sub (i32.load (global.get $input_len_idx)) (global.get $input_read_idx)))

        ;; если все прочитали, но есть что еще прочесть
        (if
          (i32.and
            (i32.eq (global.get $input_eof) (i32.const 0))
            (i32.le_s (local.get $_input_unprocessed) (global.get $min_possible_buffer_reserve)))
          (block
            (global.set $state (global.get $state_need_more_data))
            (br $_block)))

        ;; если все прочитали, и нечего прочесть
        (if
          (i32.and
            (i32.eq (global.get $input_eof) (i32.const 1))
            (i32.le_s (local.get $_input_unprocessed) (i32.const 0)))
          (block
            (global.set $state (global.get $state_cell_and_eof))
            (br $_block)))

        ;; если встретили разделитель колонки
        (local.set $i (i32.const 0))
        (local.set $res (i32.const 0))
        (local.set $a (global.get $col_sep_idx))
        (local.set $b (i32.add (global.get $input_idx) (global.get $input_read_idx)))
        (local.set $max (i32.load (global.get $col_sep_len_idx)))
        (block $_block
          (loop $_loop
            (if (i32.ge_u (local.get $i) (local.get $max))
              (block
                (local.set $res (i32.const 1))
                (br $_block)))

            (if
              (i32.ne
                (i32.load8_u (i32.add (local.get $a) (local.get $i)))
                (i32.load8_u (i32.add (local.get $b) (local.get $i))))
              (block
                (br $_block)))

            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br $_loop))

          (unreachable))
        (if
          (local.get $res)
          (block
            (global.set $state (global.get $state_cell))
            (br $_block)))

        ;; если встретили перенос строки
        (local.set $i (i32.const 0))
        (local.set $res (i32.const 0))
        (local.set $a (global.get $line_sep_idx))
        ;; (local.set $b (i32.add (global.get $input_idx) (global.get $input_read_idx)))
        (local.set $max (i32.load (global.get $line_sep_len_idx)))
        (block $_block
          (loop $_loop
            (if (i32.ge_u (local.get $i) (local.get $max))
              (block
                (local.set $res (i32.const 1))
                (br $_block)))

            (if
              (i32.ne
                (i32.load8_u (i32.add (local.get $a) (local.get $i)))
                (i32.load8_u (i32.add (local.get $b) (local.get $i))))
              (block
                (br $_block)))

            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br $_loop))

          (unreachable))
        (if
          (local.get $res)
          (block
            (global.set $state (global.get $state_cell_and_newline))
            (br $_block)))

        ;; если не хватает памяти
        ;; (if (call $need_more_memory)
        ;;   (block
        ;;     (global.set $state (global.get $state_error))
        ;;     (if (i32.ne (memory.grow (i32.const 1)) (i32.const -1))
        ;;       (global.set $state (i32.const 0)))
        ;;     (br $_loop)))

        ;; если есть что прочитать
        (if
          (i32.gt_s (local.get $_input_unprocessed) (i32.const 0))
          (block
            (i32.store8
              (i32.add (global.get $cell_idx) (global.get $cell_write_idx))
              (i32.load8_u (i32.add (global.get $input_idx) (global.get $input_read_idx))))
            (global.set $cell_write_idx (i32.add (global.get $cell_write_idx) (i32.const 1)))
            (global.set $input_read_idx (i32.add (global.get $input_read_idx) (i32.const 1)))
            (br $_loop)))

        (unreachable))
      (unreachable))
    (global.get $state))
)
