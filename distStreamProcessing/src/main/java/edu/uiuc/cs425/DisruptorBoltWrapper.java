package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.SerializationUtils;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorBoltWrapper {
	
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
	
	// event handler. consumer
	public class TupleEventHandler implements EventHandler<TupleEvent> {

		private IBolt m_oBolt;
		
		public TupleEventHandler(IBolt bolt)
		{
			m_oBolt = bolt;
		}

	    public void onEvent(TupleEvent tupleEvent, long sequence, boolean endOfBatch) throws Exception {
	        if (tupleEvent != null && tupleEvent.get() != null) {
	        	Tuple tuple = tupleEvent.get();
	            m_oBolt.execute(tuple);
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
	private Disruptor<TupleEvent>   m_oDisruptor;
	//public methods
	public DisruptorBoltWrapper(int nRingBufferSize, IBolt oBolt)
	{
		m_nRingBufferSize 	= nRingBufferSize;
		m_oBolt				= oBolt;
	}
	
	public void Init()
	{
		Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        TupleEventFactory factory = new TupleEventFactory();

        // Construct the Disruptor
        m_oDisruptor = new Disruptor<TupleEvent>(factory, m_nRingBufferSize, executor);

        // Connect the handler
        m_oDisruptor.handleEventsWith(new TupleEventHandler(m_oBolt));

        // Start the Disruptor, starts all threads running
        m_oDisruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<TupleEvent> ringBuffer = m_oDisruptor.getRingBuffer();

        m_oProducer = new TupleEventProducer(ringBuffer);
	}
	
	public void WriteData(Tuple tuple)
	{
		m_oProducer.onData(tuple);
	}
	
	public void close()
	{
		if(m_oDisruptor != null)
		{
			m_oDisruptor.shutdown();
		}
	}
}
