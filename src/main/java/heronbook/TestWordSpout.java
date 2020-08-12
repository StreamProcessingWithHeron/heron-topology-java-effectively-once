/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package heronbook;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;

import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;

import org.pmw.tinylog.*;
import org.pmw.tinylog.writers.*;

public class TestWordSpout extends BaseRichSpout
  implements IStatefulComponent<String, Integer> { 

    private Map<String, Integer> myMap; 
    private State<String, Integer> myState; 

    boolean isDistributed;
    SpoutOutputCollector collector;

    public TestWordSpout() {
        this(true);
    }

    public TestWordSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public static void setLoggging(TopologyContext context) {
      LoggingContext.put("tId", context.getThisTaskId()); 
      LoggingContext.put("cId", context.getThisComponentId()); 
      LoggingContext.put("tIdx", context.getThisTaskIndex()); 
      Configurator.currentConfig()
        .formatPattern("{date:yyyy-MM-dd HH:mm:ss} "
          + "/{context:tId}({context:cId}[{context:tIdx}])/ " 
          + "[{thread}]\n{class}.{method}() {level}:\n{message}")
        .writer(new SharedFileWriter("/tmp/log.txt", true), 
                Level.TRACE)
        .activate();
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
      this.collector = collector;
      setLoggging(context);
      Logger.trace("streams: {}", context.getThisStreams());

      myMap = new HashMap<String, Integer>();
      for (String key: myState.keySet()) {
          myMap.put(key, myState.get(key)); 
      }
      Logger.trace("position map {}", myMap);
    }

    @Override
    public void close() {
      Logger.trace("close");
    }

    @Override
    public void nextTuple() {
      Utils.sleep(1000);
      final String[] words = new String[] {
        "nathan", "mike", "jackson", "golda", "bertels"};
      int offset = myMap.getOrDefault("current", 0);
      final String word = words[offset];
      myMap.put("current", ++offset % words.length); 
      collector.emit(new Values(word));
      Logger.trace("position map {}", myMap);
    }

    @Override
    public void initState(State<String, Integer> state) { 
      this.myState = state; 
      Logger.trace("position state {}", myState);
    }
    
    @Override
    public void preSave(String checkpointId) { 
      for (String key : myMap.keySet()) {
        myState.put(key, myMap.get(key)); 
      }
      Logger.trace("position state {}", myState);
    }
  
    @Override
    public void ack(Object msgId) {
      Logger.trace("ack {}", msgId);
    }

    @Override
    public void fail(Object msgId) {
      Logger.trace("fail {}", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream("s1", new Fields("f1"));
      declarer.declareStream("s2", new Fields("f2", "f3"));
    }
}
