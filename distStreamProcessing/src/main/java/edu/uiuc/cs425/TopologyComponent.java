package edu.uiuc.cs425;

public class TopologyComponent {
	
	private String 				m_sComponentName;
	private String				m_sParentName;
	private String				m_sGroupingType;
	private int					m_nParallelismLevel;
	
	public String getM_sComponentName() {
		return m_sComponentName;
	}
	public void setM_sComponentName(String m_sComponentName) {
		this.m_sComponentName = m_sComponentName;
	}
	public String getM_sParentName() {
		return m_sParentName;
	}
	public void setM_sParentName(String m_sParentName) {
		this.m_sParentName = m_sParentName;
	}
	public String getM_sGroupingType() {
		return m_sGroupingType;
	}
	public void setM_sGroupingType(String m_sGroupingType) {
		this.m_sGroupingType = m_sGroupingType;
	}
	public int getM_nParallelismLevel() {
		return m_nParallelismLevel;
	}
	public void setM_nParallelismLevel(int m_nParallelismLevel) {
		this.m_nParallelismLevel = m_nParallelismLevel;
	}
}
