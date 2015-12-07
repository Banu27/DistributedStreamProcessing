package edu.uiuc.cs425;

public class TopologyComponent {
	
	private String 				m_sComponentName;
	private String				m_sClassName;
	private TopologyComponent	m_oParent;
	private int 				m_nGroupingType;
	private int					m_nParallelismLevel;
	private int					m_nCompType;
	private String 				m_sFieldGroup;
	private long				m_nCurrTupleCount;
	
	public TopologyComponent(String comp, String className, int compType, TopologyComponent parent, 
			int type, String sField, int parLevel)
	{
		m_sComponentName = comp;
		m_sClassName = className;
		m_oParent = parent; 
		m_nGroupingType = type;
		m_nParallelismLevel = parLevel;
		m_nCompType = compType;
		m_sFieldGroup = sField;
		m_nCurrTupleCount = 0;
	}
	
	public String getFieldGroup()
	{
		return m_sFieldGroup;
	}
	
	public long nextTupleIndex()
	{
		return m_nCurrTupleCount++ % 1000;
	}
	
	public int getCompType()
	{
		return m_nCompType;
	}
	
	public String getClassName()
	{
		return m_sClassName;
	}
	
	public String getComponentName() {
		return m_sComponentName;
	}
	
	public TopologyComponent getParent() {
		return m_oParent;
	}
	
	public int getGroupingType() {
		return m_nGroupingType;
	}
	
	public int getParallelismLevel() {
		return m_nParallelismLevel;
	}
	
}
