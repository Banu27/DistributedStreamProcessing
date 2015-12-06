package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.SerializationUtils;
import org.w3c.dom.Node;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorWrapper {
	
	// nested class def
	
	// the event data
	public class TupleEvent {

	    private Tuple message;

	    public void set(Tuple message){
	        this.message = message;
	    }

	    public Tuple get() {
	        return this.message;
	    }
	}

	// event factory
	public class TupleEventFactory implements EventFactory<TupleEvent> {

	    public TupleEvent newInstance() {
	        return new TupleEvent();
	    }
	}
	
	// event handler. bolt consumer
	public class TupleBoltEventHandler implements EventHandler<TupleEvent> {

		private IBolt m_oBolt;
		
		public TupleBoltEventHandler(IBolt bolt)
		{
			m_oBolt = bolt;
		}

	    public void onEvent(TupleEvent tupleEvent, long sequence, boolean endOfBatch) throws Exception {
	        if (tupleEvent != null && tupleEvent.get() != null) {
	        	System.out.println("Received tuple at bolt");
	        	Tuple tuple = tupleEvent.get();
	            m_oBolt.execute(tuple);
	        }
	    }
	}
	
	// event handler. node input consumer
	public class TupleNodeInputEventHandler implements EventHandler<TupleEvent> {

		private NodeManager m_oNM;
		
		public TupleNodeInputEventHandler(NodeManager obj)
		{
			m_oNM = obj;
		}

	    public void onEvent(TupleEvent tupleEvent, long sequence, boolean endOfBatch) throws Exception {
	        if (tupleEvent != null && tupleEvent.get() != null) {
	        	System.out.println("Received tuple at Node input");
	        	Tuple tuple = tupleEvent.get();
	        	m_oNM.SendTupleToTask(tuple);	        	
	        }
	    }
	}
	
	// event handler. node input consumer
	public class TupleNodeOutputEventHandler implements EventHandler<TupleEvent> {

		private NodeManager m_oNodeMgr;
		
		public TupleNodeOutputEventHandler(NodeManager obj)
		{
			m_oNodeMgr = obj;
		}

	    public void onEvent(TupleEvent tupleEvent, long sequence, boolean endOfBatch) throws Exception {
	        if (tupleEvent != null && tupleEvent.get() != null) {
	        	Tuple tuple = tupleEvent.get();
	        	System.out.println("Received tuple at Node output");
	        	// the tuple should go to the nodemanager and 
	        	// forwarded to the method that decides where the 
	        	// next stop of the tuple is.
	        	
	        	m_oNodeMgr.SendToNextComponent(tuple);
	        }
	    }
	}

	// writer class
	public class TupleEventProducer
	{
	    private final RingBuffer<TupleEvent> ringBuffer;

	    public TupleEventProducer(RingBuffer<TupleEvent> ringBuffer)
	    {
	        this.ringBuffer = ringBuffer;
	    }

	    private final EventTranslatorOneArg<TupleEvent, Tuple> TRANSLATOR =
	        new EventTranslatorOneArg<TupleEvent, Tuple>()
	        {
	            public void translateTo(TupleEvent event, long sequence, Tuple tuple)
	            {
	                event.set(tuple);
	            }
	        };

	    public void onData(Tuple tuple)
	    {
	        ringBuffer.publishEvent(TRANSLATOR, tuple);
	    }
	}
	
	// private members

	private int		 				m_nRingBufferSize;
	private IBolt 					m_oBolt;
	private TupleEventProducer		m_oProducer;
	private NodeManager 			m_oNM;
	private String                  m_sName;
	
	private Disruptor<TupleEvent>   m_oDisruptor;
	
	//public methods
	public DisruptorWrapper(int nRingBufferSize)
	{
		m_nRingBufferSize 	= nRingBufferSize;
		
	}
	
	// only one of the three init methods should be called
	// init method for bolt disruptor
	public void InitBolt(IBolt oBolt)
	{
		m_oBolt				= oBolt;
		Executor executor = Executors.newSingleThreadExecutor();

        // The factory for the event
        TupleEventFactory factory = new TupleEventFactory();

        // Construct the Disruptor
        m_oDisruptor = new Disruptor<TupleEvent>(factory, m_nRingBufferSize, executor);

        // Connect the handler
        m_oDisruptor.handleEventsWith(new TupleBoltEventHandler(m_oBolt));

        // Start the Disruptor, starts all threads running
        m_oDisruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<TupleEvent> ringBuffer = m_oDisruptor.getRingBuffer();

        m_oProducer = new TupleEventProducer(ringBuffer);
        m_sName = "Bolt input queue";
	}
	
	// init for nodeinput disruptor
	public void InitNodeInput(NodeManager obj)
	{
		m_oNM = obj;
		Executor executor = Executors.newSingleThreadExecutor();

        // The factory for the event
        TupleEventFactory factory = new TupleEventFactory();

        // Construct the Disruptor
        m_oDisruptor = new Disruptor<TupleEvent>(factory, m_nRingBufferSize, executor);

        // Connect the handler
        m_oDisruptor.handleEventsWith(new TupleNodeInputEventHandler(m_oNM));

        // Start the Disruptor, starts all threads running
        m_oDisruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<TupleEvent> ringBuffer = m_oDisruptor.getRingBuffer();

        m_oProducer = new TupleEventProducer(ringBuffer);
        m_sName = "Node input queue";
	}
	
	
	// init for nodeinput disruptor
	public void InitNodeOutput(NodeManager obj)
	{
		m_oNM = obj;
		Executor executor = Executors.newSingleThreadExecutor();

        // The factory for the event
        TupleEventFactory factory = new TupleEventFactory();

        // Construct the Disruptor
        m_oDisruptor = new Disruptor<TupleEvent>(factory, m_nRingBufferSize, executor);

        // Connect the handler
        m_oDisruptor.handleEventsWith(new TupleNodeOutputEventHandler(m_oNM));

        // Start the Disruptor, starts all threads running
        m_oDisruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<TupleEvent> ringBuffer = m_oDisruptor.getRingBuffer();

        m_oProducer = new TupleEventProducer(ringBuffer);
        m_sName = "Node output queue";
	}
	
	
	public void WriteData(Tuple tuple)
	{
		m_oProducer.onData(tuple);
		System.out.println("Writing data to disruptor: " + m_sName);
	}
	
	public void close()
	{
		if(m_oDisruptor != null)
		{
			m_oDisruptor.shutdown();
		}
	}
}
