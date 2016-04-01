/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.tilt.minka.domain.ShardEntity;

public class ShardDutyTest {
    
    @Test
    public void tgst() {
        int i = (int) Math.ceil((double)20/3);
        System.out.println(i);
        Assert.assertTrue(true);
    }
    //@Test
    public void testBasic() {
        int i = (int) Math.ceil((double)20/3);
        System.out.println(i);
        Assert.assertTrue(true);

        
        Set<ShardEntity> set = new HashSet<>();
        DemoDuty d2 = new DemoDuty("p0");
        set.add(ShardEntity.create(new DemoDuty("p0")));
        set.add(ShardEntity.create(d2));
        set.add(ShardEntity.create(new DemoDuty("p0")));
        assert(set.size() == 3);
        
        Assert.assertTrue(set.contains(ShardEntity.create(d2)));
    }
    
}
