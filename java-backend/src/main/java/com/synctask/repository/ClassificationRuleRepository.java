package com.synctask.repository;

import com.synctask.entity.ClassificationRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ClassificationRuleRepository extends JpaRepository<ClassificationRule, Long> {

    java.util.List<ClassificationRule> findByEnabledTrueOrderByPriorityAsc();
    java.util.Optional<ClassificationRule> findByName(String name);
}
