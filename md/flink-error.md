# Flink 1.7.2 Error 收集


### Failed to construct terminal; falling back to unsupported
- Failed to construct terminal; falling back to unsupported
- java.lang.NumberFormatException: For input string: "0x100"

- 解决办法

```aidl
export TERM=xterm-color

```

```aidl

[ERROR] Failed to construct terminal; falling back to unsupported
java.lang.NumberFormatException: For input string: "0x100"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Integer.parseInt(Integer.java:580)
	at java.lang.Integer.valueOf(Integer.java:766)
	at scala.tools.jline_embedded.internal.InfoCmp.parseInfoCmp(InfoCmp.java:59)
	at scala.tools.jline_embedded.UnixTerminal.parseInfoCmp(UnixTerminal.java:242)
	at scala.tools.jline_embedded.UnixTerminal.<init>(UnixTerminal.java:65)
	at scala.tools.jline_embedded.UnixTerminal.<init>(UnixTerminal.java:50)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at java.lang.Class.newInstance(Class.java:442)
	at scala.tools.jline_embedded.TerminalFactory.getFlavor(TerminalFactory.java:211)
	at scala.tools.jline_embedded.TerminalFactory.create(TerminalFactory.java:102)
	at scala.tools.jline_embedded.TerminalFactory.get(TerminalFactory.java:186)
	at scala.tools.jline_embedded.TerminalFactory.get(TerminalFactory.java:192)
	at scala.tools.jline_embedded.console.ConsoleReader.<init>(ConsoleReader.java:243)
	at scala.tools.jline_embedded.console.ConsoleReader.<init>(ConsoleReader.java:235)
	at scala.tools.jline_embedded.console.ConsoleReader.<init>(ConsoleReader.java:223)
	at scala.tools.nsc.interpreter.jline_embedded.JLineConsoleReader.<init>(JLineReader.scala:64)
	at scala.tools.nsc.interpreter.jline_embedded.InteractiveReader.<init>(JLineReader.scala:33)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$scala$tools$nsc$interpreter$ILoop$$instantiater$1$1.apply(ILoop.scala:858)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$scala$tools$nsc$interpreter$ILoop$$instantiater$1$1.apply(ILoop.scala:855)
	at scala.tools.nsc.interpreter.ILoop.scala$tools$nsc$interpreter$ILoop$$mkReader$1(ILoop.scala:862)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$22$$anonfun$apply$10.apply(ILoop.scala:873)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$22$$anonfun$apply$10.apply(ILoop.scala:873)
	at scala.util.Try$.apply(Try.scala:192)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$22.apply(ILoop.scala:873)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$22.apply(ILoop.scala:873)
	at scala.collection.immutable.Stream$$anonfun$map$1.apply(Stream.scala:418)
	at scala.collection.immutable.Stream$$anonfun$map$1.apply(Stream.scala:418)
	at scala.collection.immutable.Stream$Cons.tail(Stream.scala:1233)
	at scala.collection.immutable.Stream$Cons.tail(Stream.scala:1223)
	at scala.collection.immutable.Stream.collect(Stream.scala:435)
	at scala.tools.nsc.interpreter.ILoop.chooseReader(ILoop.scala:875)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1$$anonfun$newReader$1$1.apply(ILoop.scala:893)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.newReader$1(ILoop.scala:893)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.scala$tools$nsc$interpreter$ILoop$$anonfun$$preLoop$1(ILoop.scala:897)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1$$anonfun$startup$1$1.apply(ILoop.scala:964)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply$mcZ$sp(ILoop.scala:990)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply(ILoop.scala:891)
	at scala.tools.nsc.interpreter.ILoop$$anonfun$process$1.apply(ILoop.scala:891)
	at scala.reflect.internal.util.ScalaClassLoader$.savingContextLoader(ScalaClassLoader.scala:97)
	at scala.tools.nsc.interpreter.ILoop.process(ILoop.scala:891)
	at org.apache.flink.api.scala.FlinkShell$.startShell(FlinkShell.scala:226)
	at org.apache.flink.api.scala.FlinkShell$.main(FlinkShell.scala:135)
	at org.apache.flink.api.scala.FlinkShell.main(FlinkShell.scala)
```