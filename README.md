# これは何

Symbolic linkを介した簡易の排他ロック実装。

対象となるファイルに対して「ファイル名.プロセス名」というファイルを作成し、
「ファイル名」から「ファイル名.プロセス名」に対するハードリンクを作成する
操作がアトミックになることを利用して簡易の排他ロックを実現する。

モジュールとして実行することで実際に動作することをテストも出来る

```
(一つ目のターミナルから)
$ python3 -m alowa_lite.lock reader

(2つ目のターミナルから)
$ python3 -m alowa_lite.lock writer
```

使用している文法の関係でPython 3でのみ動作する。
動作確認はPython 3.4.6, 3.6.2 (on macOS Sierra)で行っている。

